use std::{
    io,
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::core::State;
use bus::Bus;

mod client;
mod core;
mod server;

const TICK_DELAY: u64 = 1000;

fn start_timer(state_mtx: Arc<Mutex<State>>) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(TICK_DELAY));
        let mut state = state_mtx.lock().unwrap();
        state.tick();
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
        client::connect(p, state_mtx.clone());
    }

    start_timer(state_mtx.clone());

    server::serve(listener, state_mtx)?;
    Ok(())
}
