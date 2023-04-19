use std::io::Read;
use futures::channel::mpsc;
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncReadExt;
use futures::StreamExt;
use std::sync::Arc;
use clap::Parser;

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

async fn do_tcp_listen(listen_host_opt : Option<&str>, listen_port : u16, is_listening_repeatedly : bool) -> std::io::Result<()> {
    // If the caller specified a specific address, listen on that. Otherwise, listen on all the unspecified addresses.
    let mut listen_addrs =
        if let Some(listen_host) = listen_host_opt {
            vec![format!("{}:{}", listen_host, listen_port).parse().or(Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)))?]
        } else {
            vec![
                std::net::SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, listen_port)),
                std::net::SocketAddr::V6(std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, listen_port, 0, 0)),
                ]
        };

    // Map the listening addresses to a set of sockets, and bind them.
    let mut listening_sockets = vec![];
    for listen_addr in listen_addrs.drain(..) {
        let listening_socket = if listen_addr.is_ipv4() { tokio::net::TcpSocket::new_v4() } else { tokio::net::TcpSocket::new_v6() }?;
        listening_socket.bind(listen_addr)?;
        listening_sockets.push(listening_socket);
    }

    // Listen on all the sockets.
    let mut listeners = vec![];
    for listening_socket in listening_sockets {
        let local_addr = listening_socket.local_addr()?;
        eprintln!("Listening on {}, protocol TCP, family {}",
            local_addr,
            if local_addr.is_ipv4() { "IPv4" } else { "IPv6" });

        listeners.push(listening_socket.listen(1)?);
    }

    loop {
        // Try to accept connections on any of the listening sockets. Handle clients one at a time. select_all waits for
        // the first accept to complete and cancels waiting on all the rest.
        let (listen_result, _i, _rest) = futures::future::select_all(listeners.iter().map(|listener| {
            Box::pin(listener.accept())
        })).await;

        match listen_result {
            Ok((stream, ref peer_addr)) => {
                eprintln!("Accepted connection from {}, protocol TCP, family {}",
                    peer_addr,
                    if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" });
                let stream_result = handle_tcp_stream(stream).await;

                // After handling a client, either loop and accept another client or exit.
                if !is_listening_repeatedly {
                    return stream_result;
                } else {
                    match stream_result {
                        Ok(_) => eprintln!("Connection from {} closed gracefully.", peer_addr),
                        Err(e) => eprintln!("Connection from {} closed with result: {}", peer_addr, e),
                    }
                }
            },
            Err(e) => {
                if !is_listening_repeatedly {
                    return Err(e);
                } else {
                    eprintln!("Failed to accept connection: {}", e);
                }
            },
        }
    }
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

#[derive(clap::Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Use UDP instead of TCP
    #[arg(short = 'u')]
    is_udp : bool,

    /// Listen for incoming connections
    #[arg(short = 'l')]
    is_listening : bool,

    /// Listen repeatedly for incoming connections (implies -l)
    #[arg(short = 'L')]
    is_listening_repeatedly : bool,

    /// Port to bind to
    #[arg(short = 'p')]
    listen_port : Option<u16>,

    /// Hostname to connect to
    hostname : Option<String>,

    /// Port number to connect on
    port : Option<u16>,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let mut args = Args::parse();

    if args.is_listening_repeatedly {
        args.is_listening = true;
    }

    let result = if args.is_listening {
        if args.is_udp {
            // TODO: implement UDP listener
            Ok(())
        } else {
            do_tcp_listen(
                // TODO: support specifying the listen address
                None,
                args.listen_port.ok_or(String::from("Need listening port"))?,
                args.is_listening_repeatedly).await
        }
    } else {
        let hostname = &args.hostname.ok_or(String::from("Need hostname to connect to"))?;
        let port = args.port.ok_or(String::from("Need port to connect to"))?;

        if args.is_udp {
            do_udp_connection(hostname, port).await
        } else {
            do_tcp_connect(hostname, port).await
        }
    };

    result.map_err(|e| { format!("{}: {}", e.kind(), e) })
}
