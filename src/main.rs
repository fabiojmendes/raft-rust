use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use bus::Bus;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

const CLUSTER_SIZE: i32 = 3;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Message {
    id: u16,
    term: u64,
    mtype: MessageType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum MessageType {
    AppendReq,
    AppendRes,
    VoteReq,
    VoteRes,
    ACK,
}

#[derive(Debug, PartialEq)]
enum Status {
    Follower,
    Candidate,
    Leader,
}

struct State {
    id: u16,
    term: u64,
    votes: i32,
    voted_on: u64,
    last_update: Instant,
    status: Status,
    bus: Bus<Message>,
}

impl State {
    fn new(id: u16, bus: Bus<Message>) -> Self {
        State {
            id,
            term: 0,
            votes: 0,
            voted_on: 0,
            last_update: Instant::now(),
            status: Status::Follower,
            bus,
        }
    }

    fn vote(&mut self, msg: Message) -> Message {
        let mtype = if msg.term > self.term && self.voted_on < msg.term {
            println!("[{}] Voting on {:?}", self.id, msg);
            self.term = msg.term;
            self.voted_on = msg.term;
            self.last_update = Instant::now();
            MessageType::VoteRes
        } else {
            MessageType::ACK
        };
        Message {
            id: self.id,
            term: self.term,
            mtype,
        }
    }

    fn req_votes(&mut self) {
        self.bus.broadcast(Message {
            id: self.id,
            term: self.term,
            mtype: MessageType::VoteReq,
        });
    }

    fn receive_vote(&mut self) {
        println!("[{}] Vote received", self.id);
        self.votes += 1;
        if self.status == Status::Candidate && self.votes >= CLUSTER_SIZE / 2 + 1 {
            println!("[{}] Becoming Leader", self.id);
            self.status = Status::Leader;
        }
    }

    fn tick(&mut self) {
        match self.status {
            Status::Follower => {
                let mut rng = thread_rng();
                let delay = Duration::from_millis(3000 + rng.gen_range(0..2000));
                if self.voted_on <= self.term && self.last_update.elapsed() >= delay {
                    println!("[{}] Becoming candidate", self.id);
                    self.status = Status::Candidate;
                    self.term += 1;
                    self.votes = 1;
                    self.voted_on = self.term;
                    self.last_update = Instant::now();
                    self.req_votes();
                }
            }
            Status::Candidate => {
                if self.last_update.elapsed() >= Duration::from_millis(3000) {
                    println!("[{}] Election failed... term:{}", self.id, self.term);
                    self.status = Status::Follower;
                    self.last_update = Instant::now();
                }
            }
            Status::Leader => {
                self.last_update = Instant::now();
                self.bus.broadcast(Message {
                    id: self.id,
                    term: self.term,
                    mtype: MessageType::AppendReq,
                });
            }
        }
    }
    fn append(&mut self, msg: Message) -> Message {
        println!("[{}] Append from {:?}", self.id, msg);
        if msg.term >= self.term {
            self.status = Status::Follower;
            self.term = msg.term;
            self.last_update = Instant::now();
        }
        Message {
            id: self.id,
            term: self.term,
            mtype: MessageType::AppendRes,
        }
    }

    fn commit(&mut self, msg: Message) {
        println!("[{}] Commit TBD {:?}", self.id, msg);
    }

    fn ack(&self) -> Message {
        Message {
            id: self.id,
            term: self.term,
            mtype: MessageType::ACK,
        }
    }
}

fn handle_conn(mut stream: TcpStream, state_mtx: Arc<Mutex<State>>) {
    let mut buf = [0; 1024];
    loop {
        let size = stream.read(&mut buf).unwrap();
        if size == 0 {
            println!("Stream is closed: {:?}", stream);
            break;
        }
        let req: Message = bincode::deserialize(&buf[..size]).unwrap();
        let res = {
            let mut state = state_mtx.lock().unwrap();
            match req.mtype {
                MessageType::AppendReq => state.append(req),
                MessageType::VoteReq => state.vote(req),
                _ => state.ack(),
            }
        };
        let payload = bincode::serialize(&res).unwrap();
        stream.write_all(&payload).unwrap();
    }
}

fn start_timer(state_mtx: Arc<Mutex<State>>) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(1000));
        let mut state = state_mtx.lock().unwrap();
        state.tick();
    });
}

fn connect(peer: u16, state_mtx: Arc<Mutex<State>>) {
    thread::spawn(move || {
        println!("Connecting to {}", peer);
        loop {
            match TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], peer))) {
                Ok(mut stream) => {
                    println!("Connected {:?}", stream);
                    let mut bus = {
                        let mut state = state_mtx.lock().unwrap();
                        state.bus.add_rx()
                    };
                    let mut buf = [0; 1024];
                    loop {
                        let msg = bus.recv().unwrap();
                        let payload = bincode::serialize(&msg).unwrap();
                        stream.write_all(&payload).unwrap();
                        let size = stream.read(&mut buf).unwrap();
                        if size == 0 {
                            println!("Connect stream was closed: {:?}", stream);
                            break;
                        }
                        let res: Message = bincode::deserialize(&buf[..size]).unwrap();
                        let mut state = state_mtx.lock().unwrap();
                        match res.mtype {
                            MessageType::AppendRes => state.commit(res),
                            MessageType::VoteRes => state.receive_vote(),
                            _ => println!("NOP {:?}", res),
                        }
                    }
                }
                Err(e) => {
                    println!("Error connecting {}", e);
                    thread::sleep(Duration::from_millis(5000));
                }
            }
        }
    });
}

fn main() -> io::Result<()> {
    let ports = [8080, 8081, 8082];
    let addrs = ports.map(|p| SocketAddr::from(([127, 0, 0, 1], p)));
    let listener = TcpListener::bind(&addrs[..])?;
    let local_port = listener.local_addr()?.port();
    let peers: Vec<_> = ports.into_iter().filter(|&p| p != local_port).collect();
    println!("[{}] Listening for {:?}", local_port, peers);

    let bus = Bus::new(16);
    let state_mtx = Arc::new(Mutex::new(State::new(local_port, bus)));

    for p in peers {
        connect(p, state_mtx.clone());
    }

    start_timer(state_mtx.clone());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state_mtx = state_mtx.clone();
                thread::spawn(move || handle_conn(stream, state_mtx));
            }
            Err(e) => println!("[{}] Error: {}", local_port, e),
        }
    }
    Ok(())
}
