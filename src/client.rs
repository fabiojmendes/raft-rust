use std::{
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::core::{Message, MessageType, State};

const RECONNECT_DELAY: u64 = 5000;

fn start_reply_thread(mut reader: TcpStream, state_mtx: Arc<Mutex<State>>) {
    thread::spawn(move || {
        println!("Starting reply thread for {:?}", reader);
        let mut buf = [0; 1024];
        loop {
            let size = reader.read(&mut buf).unwrap();
            if size == 0 {
                println!("Connect stream was closed: {:?}", reader);
                break;
            }
            let res: Message = bincode::deserialize(&buf[..size]).unwrap();
            let mut state = state_mtx.lock().unwrap();
            match res.mtype() {
                MessageType::AppendRes { .. } => state.commit(res),
                MessageType::VoteRes => state.receive_vote(),
                _ => println!("NOP: {:?}", res),
            }
        }
    });
}

fn start_sender(mut writer: TcpStream, state_mtx: Arc<Mutex<State>>) -> Result<(), Box<dyn Error>> {
    let bus = {
        let mut state = state_mtx.lock().unwrap();
        state.add_rx()
    };
    for msg in bus {
        let payload = bincode::serialize(msg.as_ref())?;
        writer.write_all(&payload)?;
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
                    let reader = stream.try_clone().unwrap();
                    start_reply_thread(reader, state_mtx.clone());
                    if let Err(e) = start_sender(stream, state_mtx.clone()) {
                        println!("Error on writing thread {}", e);
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
