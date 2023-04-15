use std::io::Read;
use futures::channel::mpsc;
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncReadExt;
use futures::StreamExt;
use std::sync::Arc;

// TODO: would be better if this were a task instead of a thread. or maybe use stream/sink
fn setup_async_stdin_reader_thread() -> mpsc::UnboundedReceiver<[u8 ; 1]> {
    // TODO: might be better to make this bounded eventually
    let (mut tx_input, rx_input) = mpsc::unbounded();
    std::thread::Builder::new().name("stdin_reader".to_string()).spawn(move || {
        let mut stdin = std::io::stdin();
        let mut read_buf = [0u8];
        while let Ok(num_bytes_read) = stdin.read(&mut read_buf) {
            if num_bytes_read == 0 {
                // EOF. Disconnect from the channel too.
                tx_input.disconnect();
                break;
            }

            if let Err(_) = tx_input.unbounded_send(read_buf) {
                break;
            }
        }
    }).expect("Failed to create stdin reader thread");

    rx_input
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

async fn tcp_connect_to_candidate(addr : std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    eprintln!("Connecting to {}", addr);

    let socket = if addr.is_ipv4() { tokio::net::TcpSocket::new_v4() } else { tokio::net::TcpSocket::new_v6() }?;
    let stream = socket.connect(addr).await?;

    let local_addr = stream.local_addr()?;
    let peer_addr = stream.peer_addr()?;
    eprintln!("Connected from {} to {}, protocol TCP, family {}",
        local_addr,
        peer_addr,
        if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" });

    Ok(stream)
}

async fn handle_tcp_stream(mut tcp_stream : tokio::net::TcpStream) -> std::io::Result<()> {
    // Start a new thread responsible for reading from stdin and sending the input over for this thread to read, so it
    // can be done concurrently with reading/writing the TCP stream.

    let mut rx_input = setup_async_stdin_reader_thread();
    let mut stdout = tokio::io::stdout();
    let (mut rx_socket, mut tx_socket) = tcp_stream.split();

    loop {
        let stdin_to_net = async {
            if let Some(b) = rx_input.next().await {
                tx_socket.write_all(&b).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        };

        let net_to_stdout = async {
            let mut b = [0u8];
            let _ = rx_socket.read(&mut b).await?;
            stdout.write_all(&b).await?;
            stdout.flush().await?;
            Ok(())
        };

        // Send stdin bytes to the TCP stream, and print out incoming bytes from the network. If any error occurs, bail
        // out. It most likely means the socket was closed. Stdin ending is not an error.
        tokio::select! {
            result = stdin_to_net => {
                // TODO can we use a macro here to remove duplication with udp?
                match result {
                    Ok(true) => {},
                    Ok(false) => {
                        eprintln!("End of stdin reached.");
                        return Ok(());
                    },
                    Err(_) => return result.and(Ok(())),
                }
            },
            result = net_to_stdout => {
                if result.is_err() {
                    return result;
                }
            },
            else => return Ok(()),
        };
    }
}

async fn do_udp_connection(hostname : &str, port : u16) -> std::io::Result<()> {
    let candidates = tokio::net::lookup_host(format!("{}:{}", hostname, port)).await?;

    // Only use the first candidate. For UDP the concept of a "connection" is limited to being the implicit recipient of
    // a `send()` call. UDP also can't determine success, because the recipient might properly receive the packet but
    // choose not to indicate any response.
    for addr in candidates {
        let socket = udp_connect_to_candidate(addr).await?;
        return handle_udp_socket(socket).await;
    }

    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Host not found."))
}

async fn udp_connect_to_candidate(addr : std::net::SocketAddr) -> std::io::Result<tokio::net::UdpSocket> {
    eprintln!("Connecting to {}", addr);

    let socket = tokio::net::UdpSocket::bind(
        if addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }).await?;

    socket.connect(addr).await?;

    let local_addr = socket.local_addr()?;
    let peer_addr = socket.peer_addr()?;
    eprintln!("Connected from {} to {}, protocol UDP, family {}",
        local_addr,
        peer_addr,
        if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" });

    Ok(socket)
}

async fn handle_udp_socket(socket : tokio::net::UdpSocket) -> std::io::Result<()> {
    // Start a new thread responsible for reading from stdin and sending the input over for this thread to read, so it
    // can be done concurrently with reading/writing the TCP stream.

    let mut rx_input = setup_async_stdin_reader_thread();
    let mut stdout = tokio::io::stdout();

    // Make two references to the socket, one for the reader and one for the writer. This is a locked, reference counted
    // object, so both tasks can access it.
    let rx_socket = Arc::new(socket);
    let tx_socket = rx_socket.clone();

    loop {
        let stdin_to_net = async {
            // TODO: allow sending larger than 1 byte datagrams
            if let Some(b) = rx_input.next().await {
                tx_socket.send(&b).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        };

        let net_to_stdout = async {
            // Arbitrary sized buffer that's larger than a normal 1500 byte MTU.
            let mut b = [0u8 ; 2048];

            let rx_bytes = rx_socket.recv(&mut b).await?;

            // If the entire buffer filled up (which is bigger than a normal MTU) then whatever is happening is
            // unsupported.
            if rx_bytes == b.len() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Received datagram larger than 2KB."));
            }

            // Output only the amount of bytes received.
            stdout.write_all(&b[..rx_bytes]).await?;

            stdout.flush().await?;
            Ok(())
        };

        // Send stdin bytes to the network, and print out incoming bytes from the network. If any error occurs, bail
        // out. It most likely means the socket was closed.
        tokio::select! {
            result = stdin_to_net => {
                // TODO can we use a macro here to remove duplication with tcp?
                match result {
                    Ok(true) => {},
                    Ok(false) => {
                        eprintln!("End of stdin reached.");
                        return Ok(());
                    },
                    Err(_) => return result.and(Ok(())),
                }
            },
            result = net_to_stdout => {
                if result.is_err() {
                    return result;
                }
            },
            else => return Ok(()),
        };
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args : Vec<String> = std::env::args().skip(1).collect();

    if args.len() < 2 {
        return Err(String::from("Need hostname and port"));
    }

    let mut is_tcp = true;
    let mut next_arg_idx = 0;
    for i in 0 .. args.len() - 2 {
        if args[i] == "-u" {
            is_tcp = false;
            next_arg_idx = i + 1;
        }
    }

    assert!(next_arg_idx <= args.len() - 2);

    let hostname = args[next_arg_idx].clone();
    next_arg_idx += 1;

    let port = args[next_arg_idx].parse::<u16>().or(Err(String::from("Failed to parse port")))?;
    next_arg_idx += 1;

    let result = if is_tcp {
        do_tcp_connect(&hostname, port).await
    } else {
        do_udp_connection(&hostname, port).await
    };

    result.map_err(|e| { format!("{}: {}", e.kind(), e) })
}
