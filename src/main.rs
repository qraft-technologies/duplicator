use std::{
    io::{BufRead, ErrorKind, Write},
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

fn read_until<R: BufRead + ?Sized, const N: usize>(
    r: &mut R,
    delim: [u8; N],
    buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let mut read = 0;
    loop {
        let (done, used) = {
            let available = match r.fill_buf() {
                Ok(n) => n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            match memchr::memchr(delim[N - 1], available) {
                Some(i) => {
                    buf.extend_from_slice(&available[..=i]);
                    let done = if (buf.len() >= delim.len())
                        && (buf[buf.len() - delim.len()..] == delim)
                    {
                        true
                    } else {
                        false
                    };
                    (done, i + 1)
                }
                None => {
                    buf.extend_from_slice(available);
                    (false, available.len())
                }
            }
        };
        r.consume(used);
        read += used;
        if done || used == 0 {
            return Ok(read);
        }
    }
}

fn tcp_rx_loop(bind_address: SocketAddrV4, txs: Vec<Sender<Vec<u8>>>) {
    const TCP_DELIMITER: [u8; 3] = [255, 13, 10];
    const BUF_SIZE: usize = 4096;

    loop {
        let tcp_listener = TcpListener::bind(bind_address).unwrap();
        println!("[rx][{bind_address}] waiting for connection...");
        if let Ok((tcp_stream, _)) = tcp_listener.accept() {
            println!("[rx][{bind_address}] connection accepted.");
            let mut reader = std::io::BufReader::new(tcp_stream);
            let mut buf = Vec::with_capacity(BUF_SIZE);

            while let Ok(read @ 1..=BUF_SIZE) = read_until(&mut reader, TCP_DELIMITER, &mut buf) {
                buf.truncate(read - TCP_DELIMITER.len());
                buf.extend_from_slice(epoch_now().to_string().as_bytes());
                buf.extend_from_slice(&TCP_DELIMITER);
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
