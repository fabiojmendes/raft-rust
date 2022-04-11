use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use crate::core::{Message, MessageType, State};

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
            match req.mtype() {
                MessageType::AppendReq { .. } => state.append(req),
                MessageType::VoteReq => state.vote(req),
                _ => state.ack(),
            }
        };
        let payload = bincode::serialize(&res).unwrap();
        stream.write_all(&payload).unwrap();
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
