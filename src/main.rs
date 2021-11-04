use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

const CLUSTER_SIZE: i32 = 3;
const PORT: u16 = 5001;
const MULTICAST_IP: [u8; 4] = [224, 0, 0, 123];

#[derive(Debug, Serialize, Deserialize)]
enum MessageType {
    AppendRequest,
    VoteRequest,
    VoteResponse,
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    addr: IpAddr,
    term: u32,
    mtype: MessageType,
}

#[derive(Debug, PartialEq)]
enum Status {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
struct Node {
    addr: IpAddr,
    status: Status,
    term: u32,
    votes: i32,
    vote_for: u32,
    last_update: Instant,
    rpc_tx: Sender<Message>,
}

impl Node {
    fn new(addr: IpAddr, rpc_tx: Sender<Message>) -> Self {
        Node {
            addr,
            status: Status::Follower,
            term: 0,
            votes: 0,
            vote_for: 0,
            last_update: Instant::now(),
            rpc_tx,
        }
    }

    fn request_votes(&self) {
        println!("[{}] Request votes", self.addr);
        self.rpc_tx
            .send(Message {
                addr: self.addr,
                term: self.term,
                mtype: MessageType::VoteRequest,
            })
            .unwrap();
    }

    fn vote(&mut self, term: u32, peer: IpAddr) {
        self.term = term;
        self.vote_for = term;
        self.last_update = Instant::now();
        self.rpc_tx
            .send(Message {
                addr: peer,
                term: self.term,
                mtype: MessageType::VoteResponse,
            })
            .unwrap();
    }

    fn heartbeat(&mut self) {
        self.last_update = Instant::now();
        self.rpc_tx
            .send(Message {
                addr: self.addr,
                term: self.term,
                mtype: MessageType::AppendRequest,
            })
            .unwrap();
    }

    fn tick(&mut self) {
        match self.status {
            Status::Follower => {
                let mut rng = thread_rng();
                let delay = Duration::from_millis(3000 + rng.gen_range(0..2000));
                if self.vote_for <= self.term && self.last_update.elapsed() >= delay {
                    println!("[{}] Becoming candidate", self.addr);
                    self.status = Status::Candidate;
                    self.term += 1;
                    self.votes = 1;
                    self.vote_for = self.term;
                    self.last_update = Instant::now();
                    self.request_votes();
                }
            }
            Status::Candidate => {
                if self.last_update.elapsed() >= Duration::from_millis(3000) {
                    println!("[{}] Election failed... {:?}", self.addr, self);
                    self.status = Status::Follower;
                    self.last_update = Instant::now();
                }
            }
            Status::Leader => {
                self.heartbeat();
            }
        }
    }

    fn handle(&mut self, msg: Message, peer: IpAddr) {
        match msg.mtype {
            MessageType::AppendRequest => {
                println!(
                    "[{}] Append Req from {}, term {} ",
                    self.addr, peer, msg.term
                );
                if msg.term >= self.term {
                    self.status = Status::Follower;
                    self.term = msg.term;
                    self.last_update = Instant::now();
                }
            }
            MessageType::VoteRequest => {
                println!("[{}] Vote Req from {}, term {}", self.addr, peer, msg.term);
                if msg.term > self.term && self.vote_for < msg.term {
                    self.vote(msg.term, peer);
                }
            }
            MessageType::VoteResponse { .. } => {
                println!("[{}] Vote Res {:?} from {}", self.addr, msg, peer);
                self.votes += 1;
                if self.status == Status::Candidate && self.votes >= CLUSTER_SIZE / 2 + 1 {
                    println!("[{}] Becoming Leader", self.addr);
                    self.status = Status::Leader;
                }
            }
        }
    }
}

fn start_timer(node_mutex: Arc<Mutex<Node>>) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(1000));
        let mut node = node_mutex.lock().unwrap();
        node.tick();
    });
}

fn main() -> io::Result<()> {
    let multicast_addr: SocketAddr = SocketAddr::from((MULTICAST_IP, PORT));
    let args: Vec<_> = std::env::args().collect();
    let self_addr = args[1].parse().unwrap();
    let (rpc_tx, rpc_rx) = mpsc::channel();
    let node_mutex = Arc::new(Mutex::new(Node::new(self_addr, rpc_tx.clone())));
    start_timer(node_mutex.clone());

    thread::spawn(move || {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        loop {
            if let Ok(msg) = rpc_rx.recv() {
                let dst = match msg.mtype {
                    MessageType::VoteResponse => SocketAddr::from((msg.addr, PORT)),
                    _ => multicast_addr
                };
                println!("[{}] Sending {:?} to {}", &self_addr, msg, &dst);
                let payload = bincode::serialize(&msg).unwrap();
                socket.send_to(&payload, &dst).unwrap();
            }
        }
    });

    let listen_addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    let socket = UdpSocket::bind(listen_addr)?;
    let interface = Ipv4Addr::new(0, 0, 0, 0);
    let mdns_addr = Ipv4Addr::new(224, 0, 0, 123);
    socket.join_multicast_v4(&mdns_addr, &interface)?;
    println!("Listening on {}", socket.local_addr()?);

    let mut buf = vec![0u8; 2048];
    loop {
        let (recv, peer) = socket.recv_from(&mut buf).unwrap();
        let mut node = node_mutex.lock().unwrap();
        if peer.ip() == node.addr {
            continue;
        }

        let msg: Message = bincode::deserialize(&buf[..recv]).unwrap();
        node.handle(msg, peer.ip());
    }
}
