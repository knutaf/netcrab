use std::io::Read;
use futures::channel::mpsc;
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncReadExt;
use futures::StreamExt;
use std::sync::Arc;
use std::net::SocketAddr;
use clap::Parser;
use clap::Args;
use clap::CommandFactory;

// A refcounted socket plus a recv buffer to accompany it, especially useful when listening for traffic on a collection
// of sockets at the same time, so each socket has its own associated receive buffer during the asynchronous operation.
struct UdpTrafficSocket {
    socket : Arc<tokio::net::UdpSocket>,
    recv_buf : [u8 ; 2000],
}

impl UdpTrafficSocket {
    fn new(socket : tokio::net::UdpSocket) -> UdpTrafficSocket {
        UdpTrafficSocket {
            socket : Arc::new(socket),
            recv_buf : [0 ; 2000], // arbitrary size, bigger than a 1500 byte datagram
        }
    }
}

enum StdinStatus {
    StillOpen,
    Closed,
}

fn format_io_err(err : std::io::Error) -> String {
    format!("{}: {}", err.kind(), err)
}

// TODO: would be better if this were a task instead of a thread. or maybe use stream/sink
fn setup_async_stdin_reader_thread() -> mpsc::UnboundedReceiver<[u8 ; 1]> {
    // Unbounded is OK because we'd rather prioritize faster throughput at the cost of more memory.
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

// Send stdin bytes to the TCP stream, and print out incoming bytes from the network. If any error occurs, bail out. It
// most likely means the socket was closed. Stdin ending is not an error.
async fn service_stdio_and_net<F1, F2>(stdin_to_net : F1, net_to_stdout : F2) -> Option<std::io::Result<()>>
where
    F1 : futures::Future<Output = std::io::Result<StdinStatus>>,
    F2 : futures::Future<Output = std::io::Result<()>> {
    tokio::select! {
        result = stdin_to_net => {
            match result {
                Ok(StdinStatus::StillOpen) => None,
                Ok(StdinStatus::Closed) => {
                    eprintln!("End of stdin reached.");
                    Some(Ok(()))
                },

                // and() is needed to transform into this function's output result type
                Err(_) => Some(result.and(Ok(()))),
            }
        },
        result = net_to_stdout => {
            if result.is_err() {
                Some(result)
            } else {
                None
            }
        },
        else => Some(Ok(())),
    }
}

async fn do_tcp_connect(hostname : &str, port : u16, source_addrs : &Vec<SocketAddr>, args : &NcArgs) -> std::io::Result<()> {
    assert!(!args.af_limit.use_v4 || !args.af_limit.use_v6);

    let candidates = tokio::net::lookup_host(format!("{}:{}", hostname, port)).await?;

    let mut candidate_count = 0;
    for addr in candidates {
        // Skip incompatible candidates from what address family the user specified.
        if args.af_limit.use_v4 && addr.is_ipv6() ||
           args.af_limit.use_v6 && addr.is_ipv4() {
            continue;
        }

        candidate_count += 1;

        match tcp_connect_to_candidate(addr, source_addrs, args.sendbuf_size).await {
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

async fn tcp_connect_to_candidate(addr : SocketAddr, source_addrs : &Vec<SocketAddr>, sendbuf_size : u32) -> std::io::Result<tokio::net::TcpStream> {
    eprintln!("Connecting to {}", addr);

    let socket = if addr.is_ipv4() { tokio::net::TcpSocket::new_v4() } else { tokio::net::TcpSocket::new_v6() }?;

    socket.set_send_buffer_size(sendbuf_size)?;

    // Bind the local socket to the first local address that matches the address family of the destination.
    let source_addr = source_addrs.iter().find(|e| { e.is_ipv4() == addr.is_ipv4() }).ok_or(std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "No matching local address matched destination host's address family"))?;
    socket.bind(*source_addr)?;

    let stream = socket.connect(addr).await?;

    let local_addr = stream.local_addr()?;
    let peer_addr = stream.peer_addr()?;
    eprintln!("Connected from {} to {}, protocol TCP, family {}",
        local_addr,
        peer_addr,
        if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" });

    Ok(stream)
}

fn get_local_addrs(local_host_opt : Option<&str>, local_port : u16, af_limit : &AfLimit) -> std::io::Result<Vec<SocketAddr>> {
    assert!(!af_limit.use_v4 || !af_limit.use_v6);

    // If the caller specified a specific address, include that. Otherwise, include all unspecified addresses.
    let mut addrs = if let Some(local_host) = local_host_opt {
        vec![format!("{}:{}", local_host, local_port).parse().or(Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)))?]
    } else {
        vec![
            SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, local_port)),
            SocketAddr::V6(std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, local_port, 0, 0)),
            ]
    };

    // If the caller specified only one address family, filter out any incompatible address families.
    let addrs = addrs.drain(..).filter(|e| {
        !(af_limit.use_v4 && e.is_ipv6() ||
          af_limit.use_v6 && e.is_ipv4())
    }).collect();

    Ok(addrs)
}

async fn do_tcp_listen(listen_addrs : &Vec<SocketAddr>, args : &NcArgs) -> std::io::Result<()> {
    // Map the listening addresses to a set of sockets, and bind them.
    let mut listening_sockets = vec![];
    for listen_addr in listen_addrs {
        let listening_socket = if listen_addr.is_ipv4() { tokio::net::TcpSocket::new_v4() } else { tokio::net::TcpSocket::new_v6() }?;
        let _ = listening_socket.set_send_buffer_size(args.sendbuf_size);
        listening_socket.bind(*listen_addr)?;
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
                if !args.is_listening_repeatedly {
                    return stream_result;
                } else {
                    match stream_result {
                        Ok(_) => eprintln!("Connection from {} closed gracefully.", peer_addr),
                        Err(e) => eprintln!("Connection from {} closed with result: {}", peer_addr, e),
                    }
                }
            },
            Err(e) => {
                if !args.is_listening_repeatedly {
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
                Ok(StdinStatus::StillOpen)
            } else {
                Ok(StdinStatus::Closed)
            }
        };

        let net_to_stdout = async {
            let mut b = [0u8];
            let _ = rx_socket.read(&mut b).await?;
            stdout.write_all(&b).await?;
            stdout.flush().await?;
            Ok(())
        };

        match service_stdio_and_net(stdin_to_net, net_to_stdout).await {
            Some(res) => return res,
            None => {},
        }
    }
}

async fn bind_udp_sockets(listen_addrs : &Vec<SocketAddr>) -> std::io::Result<Vec<UdpTrafficSocket>> {
    // Map the listening addresses to a set of sockets, and bind them.
    let mut listening_sockets = vec![];
    for listen_addr in listen_addrs {
        listening_sockets.push(UdpTrafficSocket::new(tokio::net::UdpSocket::bind(*listen_addr).await?));

        eprintln!("Bound UDP socket to {}, family {}",
            listen_addr,
            if listen_addr.is_ipv4() { "IPv4" } else { "IPv6" });
    }

    if listening_sockets.len() == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "Could not bind any socket."));
    }

    Ok(listening_sockets)
}

async fn do_udp_connection(hostname : &str, port : u16, source_addrs : &Vec<SocketAddr>, args : &NcArgs) -> std::io::Result<()> {
    assert!(!args.af_limit.use_v4 || !args.af_limit.use_v6);

    let candidates = tokio::net::lookup_host(format!("{}:{}", hostname, port)).await?;

    // Only use the first candidate. For UDP the concept of a "connection" is limited to being the implicit recipient of
    // a `send()` call. UDP also can't determine success, because the recipient might properly receive the packet but
    // choose not to indicate any response.
    for addr in candidates {
        // Skip incompatible candidates from what address family the user specified.
        if args.af_limit.use_v4 && addr.is_ipv6() ||
           args.af_limit.use_v6 && addr.is_ipv4() {
            continue;
        }

        let socket = udp_connect_to_candidate(addr, source_addrs).await?;
        return handle_udp_outbound_connection(socket, args.sendbuf_size).await;
    }

    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Host not found."))
}

async fn udp_connect_to_candidate(addr : SocketAddr, source_addrs : &Vec<SocketAddr>) -> std::io::Result<UdpTrafficSocket> {

    // Filter the source address list to exactly one, which matches the address family of the destination.
    let source_addrs : Vec<SocketAddr> = source_addrs.iter().filter_map(|e| {
        if e.is_ipv4() == addr.is_ipv4() {
            Some(*e)
        } else {
            None
        }
    }).take(1).collect();
    assert!(source_addrs.len() == 1);

    // Bind to a single socket that matches the address family of the target address.
    let mut sockets = bind_udp_sockets(&source_addrs).await?;
    assert!(sockets.len() == 1);

    let socket = sockets.swap_remove(0);

    socket.socket.connect(addr).await?;

    let local_addr = socket.socket.local_addr()?;
    let peer_addr = socket.socket.peer_addr()?;
    eprintln!("Associating UDP socket from {} to {}, family {}",
        local_addr,
        peer_addr,
        if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" });

    Ok(socket)
}

async fn do_udp_listen(listen_addrs : &Vec<SocketAddr>, args : &NcArgs) -> std::io::Result<()> {
    let mut listeners = bind_udp_sockets(&listen_addrs).await?;

    // Pull out chunks of the specified datagram size to make full datagrams to send.
    let mut rx_chunks = setup_async_stdin_reader_thread().map(|b| b[0]).chunks(args.sendbuf_size as usize);

    let mut stdout = tokio::io::stdout();

    // Track the list of known remote peers who have sent traffic to one of our listening sockets.
    let mut known_peers = std::collections::HashSet::<SocketAddr>::new();

    // Track the target peer to use for send operations. Before any remote peer sends us traffic, it is none.
    let mut next_target_peer : Option<(Arc<tokio::net::UdpSocket>, SocketAddr)> = None;

    loop {
        // Need to make a copy of it for use in this async operation, because it's set from from the recv path.
        let target_peer = next_target_peer.clone();
        let stdin_to_net = async {
            if let Some((tx_socket, peer_addr)) = target_peer {
                if let Some(b) = rx_chunks.next().await {
                    tx_socket.send_to(&b, peer_addr).await?;
                    Ok(StdinStatus::StillOpen)
                } else {
                    Ok(StdinStatus::Closed)
                }
            } else {
                // No incoming UDP traffic yet, so no idea where to send outbound packets.
                Ok(StdinStatus::StillOpen)
            }
        };

        let net_to_stdout = async {
            // Listen for incoming UDP packets on all sockets at the same time. select_all waits for the first
            // recv_from to complete and cancels waiting on all the rest.
            let (listen_result, i, _) = futures::future::select_all(listeners.iter_mut().map(|listener| {
                Box::pin(listener.socket.recv_from(&mut listener.recv_buf))
            })).await;

            match listen_result {
                Ok((rx_bytes, peer_addr)) => {
                    let recv_buf = listeners[i].recv_buf;

                    // Set the destination peer address for stdin traffic to the first one we received data from.
                    if next_target_peer.is_none() {
                        next_target_peer = Some((listeners[i].socket.clone(), peer_addr.clone()));
                    }

                    // Only print out the first time receiving a datagram from a given peer.
                    if known_peers.insert(peer_addr.clone()) {
                        eprintln!("Received UDP datagram from {}, family {}",
                            peer_addr,
                            if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" });
                    }

                    // If the entire buffer filled up (which is bigger than a normal MTU) then whatever is happening is
                    // unsupported.
                    if rx_bytes == recv_buf.len() {
                        eprintln!("Dropping datagram with unsupported size, larger than 2KB!");
                        Ok(())
                    } else {
                        // Output only the amount of bytes received.
                        stdout.write_all(&recv_buf[..rx_bytes]).await?;

                        stdout.flush().await?;
                        Ok(())
                    }
                },
                Err(e) => {
                    // Sometimes send failures show up in the receive path for some reason. If this happens, then
                    // assume we got a failure from the currently selected target peer. Clear it so that it can be set
                    // again on the next incoming datagram.
                    next_target_peer = None;

                    if !args.is_listening_repeatedly {
                        Err(e)
                    } else {
                        eprintln!("Failed to receive UDP datagram: {}", e);
                        Ok(())
                    }
                },
            }
        };

        match service_stdio_and_net(stdin_to_net, net_to_stdout).await {
            Some(res) => return res,
            None => {},
        }
    }
}

async fn handle_udp_outbound_connection(mut socket : UdpTrafficSocket, datagram_size : u32) -> std::io::Result<()> {
    // Start a new thread responsible for reading from stdin and sending the input over for this thread to read, so it
    // can be done concurrently with reading/writing the UDP datagrams.
    // Pull out chunks of the specified datagram size to make full datagrams to send.
    let mut rx_chunks = setup_async_stdin_reader_thread().map(|b| b[0]).chunks(datagram_size as usize);

    let mut stdout = tokio::io::stdout();

    // Make a new reference to the socket for the writer. This is a locked, reference counted object, so both tasks can
    // access it.
    let tx_socket = socket.socket.clone();

    loop {
        let stdin_to_net = async {
            if let Some(b) = rx_chunks.next().await {
                tx_socket.send(&b).await?;
                Ok(StdinStatus::StillOpen)
            } else {
                Ok(StdinStatus::Closed)
            }
        };

        let net_to_stdout = async {
            // Arbitrary sized buffer that's larger than a normal 1500 byte MTU.
            let rx_bytes = socket.socket.recv(&mut socket.recv_buf).await?;

            // If the entire buffer filled up (which is bigger than a normal MTU) then whatever is happening is
            // unsupported.
            if rx_bytes == socket.recv_buf.len() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Received datagram larger than 2KB."));
            }

            // Output only the amount of bytes received.
            stdout.write_all(&socket.recv_buf[..rx_bytes]).await?;

            stdout.flush().await?;
            Ok(())
        };

        match service_stdio_and_net(stdin_to_net, net_to_stdout).await {
            Some(res) => return res,
            None => {},
        }
    }
}


#[derive(Args)]
#[group(required = false, multiple = false)]
struct AfLimit {
    /// Use IPv4 only
    #[arg(short = '4')]
    use_v4 : bool,

    /// Use IPv6 only
    #[arg(short = '6')]
    use_v6 : bool,
}

#[derive(clap::Parser)]
#[command(author, version, about, long_about = None)]
struct NcArgs {
    /// Use UDP instead of TCP
    #[arg(short = 'u')]
    is_udp : bool,

    /// Listen for incoming connections
    #[arg(short = 'l')]
    is_listening : bool,

    /// Listen repeatedly for incoming connections (implies -l)
    #[arg(short = 'L')]
    is_listening_repeatedly : bool,

    /// Source address to bind to
    #[arg(short = 's')]
    source_host : Option<String>,

    // Unspecified local port uses port 0, which when bound to assigns from the ephemeral port range.
    /// Port to bind to
    #[arg(short = 'p', default_value_t = 0)]
    source_port : u16,

    /// Send buffer/datagram size
    #[arg(long = "sb", default_value_t = 1)]
    sendbuf_size : u32,

    /// Hostname to connect to
    hostname : Option<String>,

    /// Port number to connect on
    port : Option<u16>,

    #[command(flatten)]
    af_limit: AfLimit,
}

fn usage(msg : &str) -> ! {
    eprintln!("Error: {}", msg);
    eprintln!();
    let _ = NcArgs::command().print_help();
    std::process::exit(1)
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let mut args = NcArgs::parse();

    if args.is_listening_repeatedly {
        args.is_listening = true;
    }

    // Converts Option<String> -> Option<&str>
    let source_host_opt = args.source_host.as_deref();

    // Common code for getting the source addresses to use, but put into a closure to call it later, only after
    // parameter validation is successful.
    let make_source_addrs = || {
        get_local_addrs(source_host_opt, args.source_port, &args.af_limit).map_err(format_io_err)
    };

    let result = if args.is_listening {
        let source_addrs = make_source_addrs()?;

        if args.is_udp {
            do_udp_listen(&source_addrs, &args).await
        } else {
            do_tcp_listen(&source_addrs, &args).await
        }
    } else {
        if args.hostname.is_none() {
            usage("Need hostname to connect to");
        }

        if args.port.is_none() {
            usage("Need port to connect to");
        }

        let hostname = args.hostname.as_ref().unwrap();
        let port = args.port.unwrap();
        let source_addrs = make_source_addrs()?;

        if args.is_udp {
            do_udp_connection(hostname, port, &source_addrs, &args).await
        } else {
            do_tcp_connect(hostname, port, &source_addrs, &args).await
        }
    };

    result.map_err(format_io_err)
}
