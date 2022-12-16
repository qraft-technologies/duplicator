use std::{
    io::{BufRead, Write},
    net::{SocketAddrV4, TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opt {
    bind_address: SocketAddrV4,
    forward_address: Vec<SocketAddrV4>,
}

fn tcp_tx_loop(forward_address: SocketAddrV4, rx: Receiver<Vec<u8>>) {
    loop {
        println!("[tx][{forward_address}] waiting for server...");
        if let Ok(mut stream) = TcpStream::connect(forward_address) {
            loop {
                let msg = rx.recv().unwrap();
                if let Err(e) = stream.write_all(&msg) {
                    eprintln!("{}", e);
                    break;
                }
            }
        }
        println!("[tx][{forward_address}] sleep 5 seoncds...");
        std::thread::sleep(Duration::from_secs(5));

        let st = SystemTime::now();
        let mut i = 0;
        while rx.try_recv().is_ok() {
            i += 1;
        }
        let elapsed = SystemTime::now().duration_since(st).unwrap().as_secs();
        println!("[tx][{forward_address}] discarded {i} messages during {elapsed} seconds.");
    }
}

fn tcp_rx_loop(bind_address: SocketAddrV4, txs: Vec<Sender<Vec<u8>>>) {
    const BUF_SIZE: usize = 4096;
    const DELIMITER: u8 = 255;

    loop {
        let tcp_listener = TcpListener::bind(bind_address).unwrap();
        println!("[rx][{bind_address}] waiting for connection...");
        if let Ok((tcp_stream, _)) = tcp_listener.accept() {
            println!("[rx][{bind_address}] connection accepted.");
            let mut reader = std::io::BufReader::new(tcp_stream);
            let mut buf = Vec::with_capacity(BUF_SIZE);

            while let Ok(read @ 1..=BUF_SIZE) = reader.read_until(DELIMITER, &mut buf) {
                buf.truncate(read - 1);
                buf.extend_from_slice(epoch_now().to_string().as_bytes());
                buf.push(DELIMITER);
                for tx in &txs {
                    tx.send(buf.to_vec()).unwrap()
                }
                buf.clear();
            }
        }
        println!("[rx][{bind_address}] sleep 5 second...");
        std::thread::sleep(Duration::from_secs(5));
    }
}

/// Returns duration since UNIX_EPOCH in millisecconds.
pub fn epoch_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("durationsince with UNIX_EPOCH never fails.")
        .as_millis()
}

fn main() {
    let Opt {
        bind_address,
        forward_address,
    } = Opt::from_args();

    let mut txs = Vec::new();
    for addr in forward_address {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || tcp_tx_loop(addr, rx));
        txs.push(tx);
    }

    tcp_rx_loop(bind_address, txs);
}
