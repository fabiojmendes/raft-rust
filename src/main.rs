use std::{
    io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
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

#[derive(Debug, Serialize, Deserialize)]
enum Message {
    AppendRequest { id: u16, term: u32 },
    VoteRequest { id: u16, term: u32 },
    VoteResponse { id: u16, peer: SocketAddr },
}

#[derive(Debug, PartialEq)]
enum Status {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
struct Node {
    id: u16,
    status: Status,
    term: u32,
    votes: i32,
    vote_for: u32,
    last_update: Instant,
}

impl Node {
    fn new(id: u16) -> Self {
        Node {
            id,
            status: Status::Follower,
            term: 0,
            votes: 0,
            vote_for: 0,
            last_update: Instant::now(),
        }
    }

    fn request_votes(&self) -> Message {
        println!("[{}] Request votes", self.id);
        Message::VoteRequest {
            id: self.id,
            term: self.term,
        }
    }

    fn heartbeat(&mut self) -> Message {
        self.last_update = Instant::now();

        Message::AppendRequest {
            id: self.id,
            term: self.term,
        }
    }

    fn tick(&mut self) -> Option<Message> {
        match self.status {
            Status::Follower => {
                let mut rng = thread_rng();
                let delay = Duration::from_millis(3000 + rng.gen_range(0..2000));
                if self.vote_for <= self.term && self.last_update.elapsed() >= delay {
                    println!("[{}] Becoming candidate", self.id);
                    self.status = Status::Candidate;
                    self.term += 1;
                    self.votes = 1;
                    self.vote_for = self.term;
                    self.last_update = Instant::now();
                    return Some(self.request_votes());
                }
            }
            Status::Candidate => {
                if self.last_update.elapsed() >= Duration::from_millis(3000) {
                    println!("[{}] Election failed... {:?}", self.id, self);
                    self.status = Status::Follower;
                    self.last_update = Instant::now();
                }
            }
            Status::Leader => {
                return Some(self.heartbeat());
            }
        }
        None
    }

    fn handle(&mut self, msg: Message, peer: SocketAddr) -> Option<Message> {
        match msg {
            Message::AppendRequest { term, .. } => {
                println!("[{}] Append Req on term {}", self.id, term);
                if term >= self.term {
                    self.status = Status::Follower;
                    self.term = term;
                    self.last_update = Instant::now();
                }
            }
            Message::VoteRequest { id, term } => {
                println!("[{}] Vote Req from {}, term {}", self.id, peer, term);
                if term > self.term && self.vote_for < term {
                    self.vote_for = term;
                    return Some(Message::VoteResponse { id, peer });
                }
            }
            Message::VoteResponse { .. } => {
                println!("[{}] Vote Res", self.id);
                self.votes += 1;
                if self.votes >= (CLUSTER_SIZE - 1) / 2 + 1 {
                    println!("[{}] Becoming Leader", self.id);
                    self.status = Status::Leader;
                }
            }
        }
        None
    }
}

fn start_timer(node_mutex: Arc<Mutex<Node>>, tx: Sender<Message>) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(1000));
        let mut node = node_mutex.lock().unwrap();
        if let Some(msg) = node.tick() {
            tx.send(msg).unwrap();
        }
    });
}

fn main() -> io::Result<()> {
    let id = 1;
    let (tx, rx) = mpsc::channel();
    let node_mutex = Arc::new(Mutex::new(Node::new(id)));
    start_timer(node_mutex.clone(), tx.clone());

    thread::spawn(move || loop {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        match rx.recv() {
            Ok(msg) => {
                let addr = match msg {
                    Message::VoteResponse { peer: mut to, .. } => {
                        to.set_port(5001);
                        to
                    }
                    _ => "224.0.0.123:5001".parse().unwrap(),
                };
                println!("[{}] Sending {:?} to {}", id, msg, &addr);
                let payload = bincode::serialize(&msg).unwrap();
                socket.send_to(&payload, &addr).unwrap();
            }
            _ => {}
        }
    });

    let socket = UdpSocket::bind("0.0.0.0:5001")?;
    let interface = Ipv4Addr::new(0, 0, 0, 0);
    let mdns_addr = Ipv4Addr::new(224, 0, 0, 123);
    socket.join_multicast_v4(&mdns_addr, &interface)?;
    println!("Listening on {}", socket.local_addr()?);

    let mut buf = vec![0u8; 2048];
    loop {
        let (recv, peer) = socket.recv_from(&mut buf).unwrap();
        let msg: Message = bincode::deserialize(&buf[..recv]).unwrap();
        let mut node = node_mutex.lock().unwrap();
        if let Some(res) = node.handle(msg, peer) {
            tx.send(res).unwrap();
        }
    }
}
