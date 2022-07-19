use std::{
    io::{BufRead, Write},
    net::{SocketAddrV4, TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender},
    time::Duration,
};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opt {
    bind_address: SocketAddrV4,
    forward_address: Vec<SocketAddrV4>,
}

fn tcp_tx_loop(forward_address: SocketAddrV4, rx: Receiver<Vec<u8>>) {
    loop {
        if let Ok(mut stream) = TcpStream::connect(forward_address) {
            loop {
                let msg = rx.recv().unwrap();
                if let Err(e) = stream.write_all(&msg) {
                    eprintln!("{}", e);
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_secs(1));
        while rx.try_recv().is_ok() {}
    }
}

fn tcp_rx_loop(bind_address: SocketAddrV4, txs: Vec<Sender<Vec<u8>>>) {
    const TCP_DELIMITER: [u8; 3] = [255, 13, 10];
    const TCP_LAST_DELIM: u8 = TCP_DELIMITER[TCP_DELIMITER.len() - 1];
    const BUF_SIZE: usize = 4096;

    let tcp_listener = TcpListener::bind(bind_address).unwrap();
    loop {
        if let Ok((tcp_stream, _)) = tcp_listener.accept() {
            let mut reader = std::io::BufReader::new(tcp_stream);
            let mut buf = Vec::with_capacity(BUF_SIZE);

            while let Ok(read @ 1..=BUF_SIZE) = reader.read_until(TCP_LAST_DELIM, &mut buf) {
                let message = buf[..read].to_vec();
                for tx in &txs {
                    tx.send(message.clone()).unwrap()
                }
                buf.clear();
            }
        }
    }
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
