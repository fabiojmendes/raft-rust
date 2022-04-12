use std::{
    error::Error,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::core::{Message, MessageType, State};

const RECONNECT_DELAY: u64 = 5000;
const BUFFER_SIZE: usize = 32;

fn start_reply_thread(stream: &TcpStream, state_mtx: Arc<Mutex<State>>) -> io::Result<()> {
    let mut reader = stream.try_clone()?;
    thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("Starting reply thread for {:?}", reader);
        let mut buf = [0; BUFFER_SIZE];
        loop {
            reader.read_exact(&mut buf)?;
            let res: Message = bincode::deserialize(&buf)?;
            let mut state = state_mtx.lock().unwrap();
            match res.mtype() {
                MessageType::AppendRes { .. } => state.commit(res),
                MessageType::VoteRes => state.receive_vote(),
                _ => println!("NOP: {:?}", res),
            }
        }
    });

    Ok(())
}

fn start_sender(mut writer: TcpStream, state_mtx: Arc<Mutex<State>>) -> Result<(), Box<dyn Error>> {
    let bus = {
        let mut state = state_mtx.lock().unwrap();
        state.add_rx()
    };
    let mut buf = [0; BUFFER_SIZE];
    for msg in bus {
        bincode::serialize_into(&mut buf[..], msg.as_ref())?;
        writer.write_all(&buf)?;
    }
    Ok(())
}

pub fn connect(peer: u16, state_mtx: Arc<Mutex<State>>) {
    thread::spawn(move || {
        println!("Connecting to {}", peer);
        loop {
            match TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], peer))) {
                Ok(stream) => {
                    println!("Connected {:?}", stream);
                    if let Err(e) = start_reply_thread(&stream, state_mtx.clone()) {
                        println!("Error creating reading thread for {peer}: {}", e);
                        break;
                    }
                    if let Err(e) = start_sender(stream, state_mtx.clone()) {
                        println!("Error on writing thread for {peer}: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    println!("Error connecting: {}", e);
                    thread::sleep(Duration::from_millis(RECONNECT_DELAY));
                }
            }
        }
    });
}

fn handle_conn(
    mut stream: TcpStream,
    state_mtx: Arc<Mutex<State>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buf = [0; BUFFER_SIZE];
    loop {
        stream.read_exact(&mut buf)?;
        let req: Message = bincode::deserialize(&buf)?;
        let res = {
            let mut state = state_mtx.lock().unwrap();
            match req.mtype() {
                MessageType::AppendReq { .. } => state.append(req),
                MessageType::VoteReq => state.vote(req),
                _ => state.ack(),
            }
        };
        bincode::serialize_into(&mut buf[..], &res)?;
        stream.write_all(&buf)?;
    }
}

pub fn serve(listener: TcpListener, state_mtx: Arc<Mutex<State>>) -> io::Result<()> {
    let local_port = listener.local_addr()?.port();

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

#[cfg(test)]
mod tests {
    use crate::{
        core::{Message, MessageType},
        networking::BUFFER_SIZE,
    };

    #[test]
    fn test_serialize_append_req() -> Result<(), bincode::Error> {
        let msg = Message {
            id: 0,
            term: 0,
            mtype: MessageType::AppendReq { msg_id: 1 },
        };
        let size = bincode::serialized_size(&msg)? as usize;
        assert!(size < BUFFER_SIZE);
        Ok(())
    }

    #[test]
    fn test_serialize_append_res() -> Result<(), bincode::Error> {
        let msg = Message {
            id: 0,
            term: 0,
            mtype: MessageType::AppendRes { msg_id: 1 },
        };
        let size = bincode::serialized_size(&msg)? as usize;
        assert!(size < BUFFER_SIZE);
        Ok(())
    }

    #[test]
    fn test_serialize_vote_req() -> Result<(), bincode::Error> {
        let msg = Message {
            id: 0,
            term: 0,
            mtype: MessageType::VoteReq,
        };
        let size = bincode::serialized_size(&msg)? as usize;
        assert!(size < BUFFER_SIZE);
        Ok(())
    }

    #[test]
    fn test_serialize_vote_res() -> Result<(), bincode::Error> {
        let msg = Message {
            id: 0,
            term: 0,
            mtype: MessageType::VoteRes,
        };
        let size = bincode::serialized_size(&msg)? as usize;
        assert!(size < BUFFER_SIZE);
        Ok(())
    }

    #[test]
    fn test_serialize_ack() -> Result<(), bincode::Error> {
        let msg = Message {
            id: 0,
            term: 0,
            mtype: MessageType::Ack,
        };
        let size = bincode::serialized_size(&msg)? as usize;
        assert!(size < BUFFER_SIZE);
        Ok(())
    }
}
