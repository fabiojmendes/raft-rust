use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bus::Bus;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

const CLUSTER_SIZE: i32 = 3;
const MAJORITY: i32 = CLUSTER_SIZE / 2 + 1;
const ELECTION_DELAY: u64 = 3000;
const ELECTION_RND: u64 = 2000;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub(crate) id: u16,
    pub(crate) term: u64,
    pub(crate) mtype: MessageType,
}

impl Message {
    pub fn mtype(&self) -> &MessageType {
        &self.mtype
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MessageType {
    AppendReq { msg_id: u64 },
    AppendRes { msg_id: u64 },
    VoteReq,
    VoteRes,
    Ack,
}

#[derive(Debug, PartialEq)]
enum Status {
    Follower,
    Candidate,
    Leader,
}

pub struct State {
    id: u16,
    term: u64,
    votes: i32,
    voted_on: u64,
    msg_id: u64,
    last_update: Instant,
    status: Status,
    bus: Bus<Arc<Message>>,
}

impl State {
    pub fn new(id: u16, bus: Bus<Arc<Message>>) -> Self {
        State {
            id,
            term: 0,
            votes: 0,
            voted_on: 0,
            msg_id: 0,
            last_update: Instant::now(),
            status: Status::Follower,
            bus,
        }
    }

    pub fn add_rx(&mut self) -> bus::BusReader<Arc<Message>> {
        self.bus.add_rx()
    }

    pub fn vote(&mut self, msg: Message) -> Message {
        let mtype = if msg.term > self.term && self.voted_on < msg.term {
            println!("[{}] Voting on {:?}", self.id, msg);
            self.term = msg.term;
            self.voted_on = msg.term;
            self.last_update = Instant::now();
            MessageType::VoteRes
        } else {
            MessageType::Ack
        };
        Message {
            id: self.id,
            term: self.term,
            mtype,
        }
    }

    fn req_votes(&mut self) {
        self.bus.broadcast(Arc::new(Message {
            id: self.id,
            term: self.term,
            mtype: MessageType::VoteReq,
        }));
    }

    pub fn receive_vote(&mut self) {
        println!("[{}] Vote received", self.id);
        self.votes += 1;
        if self.status == Status::Candidate && self.votes >= MAJORITY {
            println!("[{}] Becoming Leader", self.id);
            self.status = Status::Leader;
        }
    }

    pub fn tick(&mut self) {
        match self.status {
            Status::Follower => {
                let mut rng = thread_rng();
                let delay = Duration::from_millis(ELECTION_DELAY + rng.gen_range(0..ELECTION_RND));
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
                if self.last_update.elapsed() >= Duration::from_millis(ELECTION_DELAY) {
                    println!("[{}] Election failed... term:{}", self.id, self.term);
                    self.status = Status::Follower;
                    self.last_update = Instant::now();
                }
            }
            Status::Leader => {
                self.last_update = Instant::now();
                self.msg_id += 1;

                self.bus.broadcast(Arc::new(Message {
                    id: self.id,
                    term: self.term,
                    mtype: MessageType::AppendReq {
                        msg_id: self.msg_id,
                    },
                }));
            }
        }
    }

    pub fn append(&mut self, msg: Message) -> Message {
        let mtype = if let MessageType::AppendReq { msg_id } = msg.mtype {
            println!("[{}] Append from {:?}", self.id, msg);
            if msg.term >= self.term {
                self.status = Status::Follower;
                self.term = msg.term;
                self.msg_id = msg_id;
                self.last_update = Instant::now();
            }
            MessageType::AppendRes { msg_id }
        } else {
            MessageType::Ack
        };

        Message {
            id: self.id,
            term: self.term,
            mtype,
        }
    }

    pub fn commit(&mut self, msg: Message) {
        if let MessageType::AppendRes { .. } = msg.mtype {
            println!("[{}] Commit {:?}", self.id, msg);
        }
    }

    pub fn ack(&self) -> Message {
        Message {
            id: self.id,
            term: self.term,
            mtype: MessageType::Ack,
        }
    }
}
