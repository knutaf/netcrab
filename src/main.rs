use std::io::Read;
use tokio::net::TcpSocket;
use futures::channel::mpsc;
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncReadExt;
use futures::StreamExt;

async fn tcp_connect_to_candidate(addr : std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    eprintln!("Connecting to {}", addr);

    let socket = if addr.is_ipv4() { TcpSocket::new_v4() } else { TcpSocket::new_v6() }?;
    let stream = socket.connect(addr).await?;

    let local_addr = stream.local_addr()?;
    let peer_addr = stream.peer_addr()?;
    eprintln!("Connected from {} to {}, protocol TCP, protocol {}",
        local_addr,
        peer_addr,
        if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" });

    Ok(stream)
}

async fn handle_tcp_stream(mut tcp_stream : tokio::net::TcpStream) -> std::io::Result<()> {
    // Start a new thread responsible for reading from stdin and sending the input over for this thread to read, so it
    // can be done concurrently with reading/writing the TCP stream.

    // TODO: eventually probably better to make this bounded
    let (tx_input, mut rx_input) = mpsc::unbounded();
    std::thread::Builder::new().name("input_reader".to_string()).spawn(move || {
        let mut stdin = std::io::stdin();
        let mut read_buf = [0u8];
        while let Ok(_) = stdin.read(&mut read_buf) {
            if let Err(_) = tx_input.unbounded_send(read_buf) {
                break;
            }
        }
    })?;

    let mut stdout = tokio::io::stdout();
    let (mut stream_reader, mut stream_writer) = tcp_stream.split();

    loop {
        let stdin_to_stream = async {
            let b = rx_input.next().await.ok_or(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "End of stdin input"))?;
            stream_writer.write_all(&b).await?;
            Ok(())
        };

        let stream_to_stdout = async {
            let b = stream_reader.read_u8().await?;
            stdout.write_all(&[b]).await?;
            stdout.flush().await?;
            Ok(())
        };

        // Send stdin bytes to the TCP stream, and print out incoming bytes from the network. If any error occurs, bail
        // out. It most likely means the socket was closed.
        tokio::select! {
            result = stdin_to_stream => {
                if result.is_err() {
                    return result;
                }
            },
            result = stream_to_stdout => {
                if result.is_err() {
                    return result;
                }
            },
            else => return Ok(()),
        };
    }
}

async fn do_tcp_connect(hostname : &str, port : u16) -> std::io::Result<()> {
    let candidates = tokio::net::lookup_host(format!("{}:{}", hostname, port)).await?;

    let mut candidate_count = 0;
    for addr in candidates {
        candidate_count += 1;

        match tcp_connect_to_candidate(addr).await {
            Ok(tcp_stream) => {
                // Return after first successful connection.
                return handle_tcp_stream(tcp_stream).await;
            },
            Err(e) => {
                eprintln!("Failed to connect to {}. Error: {}", addr, e);
            },
        }
    }

    if candidate_count == 0 {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Host not found."))
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "Failed to connect to all candidates."))
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args : Vec<String> = std::env::args().skip(1).collect();

    if args.len() < 2 {
        return Err(String::from("Need hostname and port"));
    }

    let hostname = args[0].clone();
    let port = args[1].parse::<u16>().or(Err(String::from("Failed to parse port")))?;
    do_tcp_connect(&hostname, port).await.map_err(|e| { format!("{}: {}", e.kind(), e) })
}
