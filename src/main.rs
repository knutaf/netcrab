// For AsRawFd/AsRawSocket shenanigans.
#![feature(trait_alias)]

use std::sync::Arc;
use std::io::Read;
use std::io::Write;
use std::io::IsTerminal;
use futures::future;
use futures::channel::mpsc;
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use futures::StreamExt;
use futures::SinkExt;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tokio_util::codec::BytesCodec;
use clap::Parser;
use clap::Args;
use clap::CommandFactory;
use sockconf::configure_socket_options;

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

// Tokio's async Stdin implementation doesn't work well for interactive uses. Ituses a blocking read, so if we notice
// the socket (not stdin) close, we can't cancel the read on stdin. The user has to hit enter to make the read call
// finish, and then the next read won't be started.
//
// This isn't very nice for interactive uses. Instead, this thread uses a separate thread for the blocking stdin read.
// If the remote socket closes then this thread can be exited without having to wait for the async operation to finish.
fn setup_async_stdin_reader_thread(chunk_size : u32, should_disable_console_char_mode : bool) -> mpsc::UnboundedReceiver<std::io::Result<bytes::Bytes>> {
    // Unbounded is OK because we'd rather prioritize faster throughput at the cost of more memory.
    let (mut tx_input, rx_input) = mpsc::unbounded();

    // If the user is interactively using the program, by default use "character mode", which inputs a character on
    // on every keystroke rather than a full line when hitting enter. However, the user can request to use normal
    // stdin mode.
    let is_char_mode = std::io::stdin().is_terminal() && !should_disable_console_char_mode;

    std::thread::Builder::new().name("stdin_reader".to_string()).spawn(move || {
        // Create storage for the input from stdin. It needs to be big enough to store the user's desired chunk size. It
        // will accumulate data until it's filled an entire chunk, at which point it will be sent and repurposed for the
        // next chunk.
        let mut read_buf = vec![0u8 ; chunk_size as usize];

        // The available buffer (pointer to next byte to write and remaining available space in the chunk).
        let mut available_buf = &mut read_buf[..];

        if is_char_mode {
            // The console crate has a function called stdout that gives you a Term object that also services input. OK.
            let mut stdio = console::Term::stdout();

            // A buffer specifically for encoding a single `char` read by the console crate.
            let mut char_buf = [0u8 ; std::mem::size_of::<char>()];
            let char_buf_len = char_buf.len();

            while let Ok(ch) = stdio.read_char() {
                // Encode the char from input as a series of bytes.
                let encoded_str = ch.encode_utf8(&mut char_buf);
                let num_bytes_read = encoded_str.len();
                assert!(num_bytes_read <= char_buf_len);

                // Echo the char back out, because `read_char` doesn't.
                let _ = stdio.write(encoded_str.as_bytes());

                // Track a slice of the remaining bytes of the just-read char that haven't been sent yet.
                let mut char_bytes_remaining = &mut char_buf[.. num_bytes_read];
                while char_bytes_remaining.len() > 0 {
                    // There might not be enough space left in the chunk to fit the whole char.
                    let num_bytes_copyable = std::cmp::min(available_buf.len(), char_bytes_remaining.len());
                    assert!(num_bytes_copyable <= available_buf.len());
                    assert!(num_bytes_copyable <= char_bytes_remaining.len());
                    assert!(num_bytes_copyable > 0);

                    // Copy as much of the char as possible into the chunk. Note that for UTF-8 characters sent in UDP
                    // datagrams, if we fail to copy the whole character into a given datagram, the resultant traffic
                    // might be... strange. Imagine having a UTF-8 character split across two datagrams. Not great, but
                    // also not lossy, so who is to say what is correct? It's not possible to fully fill every UDP
                    // datagram of arbitrary size with varying size UTF-8 characters and not sometimes slice them.
                    available_buf[.. num_bytes_copyable].copy_from_slice(&char_bytes_remaining[.. num_bytes_copyable]);

                    // Update the available buffer to the remaining space in the chunk after copying in this char.
                    available_buf = &mut available_buf[num_bytes_copyable ..];

                    // Advance the remaining slice of the char to the uncopied portion.
                    char_bytes_remaining = &mut char_bytes_remaining[num_bytes_copyable ..];

                    // There's no more available buffer in this chunk, meaning we've accumulated a full chunk, so send
                    // it now.
                    if available_buf.len() == 0 {
                        // Stop borrowing read_buf for a hot second so it can be sent.
                        available_buf = &mut[];

                        if let Err(_) = tx_input.unbounded_send(Ok(bytes::Bytes::copy_from_slice(&read_buf))) {
                            break;
                        }

                        // The chunk was sent. Reset the available buffer to allow storing the net chunk.
                        available_buf = &mut read_buf[..];
                    }
                }
            }
        } else {
            let mut stdin = std::io::stdin();

            while let Ok(num_bytes_read) = stdin.read(available_buf) {
                if num_bytes_read == 0 {
                    // EOF. Disconnect from the channel too.
                    tx_input.disconnect();
                    break;
                }

                assert!(num_bytes_read <= available_buf.len());

                // We've accumulated a full chunk, so send it now.
                if num_bytes_read == available_buf.len() {
                    if let Err(_) = tx_input.unbounded_send(Ok(bytes::Bytes::copy_from_slice(&read_buf))) {
                        break;
                    }

                    // The chunk was sent. Reset the buffer to allow storing the net chunk.
                    available_buf = &mut read_buf[..];
                } else {
                    // Read buffer isn't full yet. Set the available buffer to the rest of the buffer just past the
                    // portion that was written to.
                    available_buf = &mut available_buf[num_bytes_read ..];
                }
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

mod sockconf {
    use std::mem::ManuallyDrop;
    use crate::NcArgs;
    use crate::configure_socket2_options;

    #[cfg(windows)]
    use std::os::windows::io::{FromRawSocket, AsRawSocket};

    #[cfg(unix)]
    use std::os::fd::{FromRawFd, AsRawFd};

    #[cfg(windows)]
    pub trait AsRawHandleType = AsRawSocket;

    #[cfg(unix)]
    pub trait AsRawHandleType = AsRawFd;

    // There isn't a clean way to get from a Tokio TcpSocket or UdpSocket to its inner socket2::Socket, which offers
    // basically all the possible setsockopts wrapped nicely. We can create a socket2::Socket out of a raw socket/fd
    // though. But the socket2::Socket takes ownership of the handle and will close it on dtor, so wrap it in
    // `ManuallyDrop`, which lets us leak it deliberately. The socket is owned properly by the tokio socket anyway, so
    // nothing is leaked.
    //
    // The way to create a socket2::Socket out of a raw handle/fd is different on Windows vs Unix, so support both ways.
    // They only differ by whether RawSocket or RawFd is used.
    //
    // SAFETY:
    // - the socket is a valid open socket at the point of this call.
    // - the socket can be closed by closesocket - this doesn't apply, since we won't be closing it here.
    pub fn configure_socket_options<S>(socket : &S, is_ipv4 : bool, args : &NcArgs) -> std::io::Result<()> 
    where S : AsRawHandleType {
        let socket2 = ManuallyDrop::new(unsafe {
            #[cfg(windows)]
            let s = socket2::Socket::from_raw_socket(socket.as_raw_socket());

            #[cfg(unix)]
            let s = socket2::Socket::from_raw_fd(socket.as_raw_fd());

            s
        });

        configure_socket2_options(&socket2, is_ipv4, args)
    }
}

fn configure_socket2_options(socket : &socket2::Socket, is_ipv4 : bool, args : &NcArgs) -> std::io::Result<()> {
    let socktype = socket.r#type()?;

    match socktype {
        socket2::Type::DGRAM => {
            socket.set_broadcast(args.is_broadcast)?;
        },
        socket2::Type::STREAM => {
            // No stream-specific options for now
        },
        _ => {
            eprintln!("Warning: unknown socket type {:?}", socktype);
        },
    }

    socket.set_send_buffer_size(args.sendbuf_size as usize)?;

    if let Some(ttl) = args.ttl_opt {
        if is_ipv4 {
            socket.set_ttl(ttl)?;
        } else {
            eprintln!("Warning: setting TTL for IPv6 socket not supported.");
        }
    }

    Ok(())
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

        match tcp_connect_to_candidate(addr, source_addrs, args).await {
            Ok(tcp_stream) => {
                if args.is_zero_io {
                    return Ok(());
                } else {
                    // Return after first successful connection.
                    return handle_tcp_stream(tcp_stream, args.should_disable_console_char_mode).await;
                }
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

async fn tcp_connect_to_candidate(addr : SocketAddr, source_addrs : &Vec<SocketAddr>, args : &NcArgs) -> std::io::Result<tokio::net::TcpStream> {
    eprintln!("Connecting to {}", addr);

    let socket = if addr.is_ipv4() { tokio::net::TcpSocket::new_v4() } else { tokio::net::TcpSocket::new_v6() }?;

    configure_socket_options(&socket, addr.is_ipv4(), args)?;

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

        configure_socket_options(&listening_socket, listen_addr.is_ipv4(), args)?;
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

                // In Zero-IO mode, immediately close the socket. Otherwise, handle it like normal.
                let stream_result = if args.is_zero_io {
                    Ok(())
                } else {
                    handle_tcp_stream(stream, args.should_disable_console_char_mode).await
                };

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

async fn handle_tcp_stream(mut tcp_stream : tokio::net::TcpStream, should_disable_console_char_mode : bool) -> std::io::Result<()> {
    let (rx_socket, tx_socket) = tcp_stream.split();

    // Start a new thread responsible for reading from stdin and sending the input over for this thread to read, so it
    // can be done concurrently with reading/writing the TCP stream.
    let mut stdin = setup_async_stdin_reader_thread(1, should_disable_console_char_mode);
    let mut stdin_to_net_sink = FramedWrite::new(tx_socket, BytesCodec::new());

    let mut stdout = FramedWrite::new(tokio::io::stdout(), BytesCodec::new());

    // Filter map Result<BytesMut, Error> stream into just a Bytes stream to match stdout Sink. On Error, log the error
    // and end the stream.
    let mut rx_net_stream = FramedRead::new(rx_socket, BytesCodec::new())
        .filter_map(|bm| match bm {
            //BytesMut into Bytes
            Ok(bm) => future::ready(Some(bm.freeze())),
            Err(e) => {
                eprintln!("failed to read from socket; error={}", e);
                future::ready(None)
            },
        })
        .map(Ok);

    // End when either the socket is closed or stdin/stdout is closed.
    tokio::select! {
        result = stdin_to_net_sink.send_all(&mut stdin) => {
            eprintln!("End of stdin reached.");
            result
        },
        result = stdout.send_all(&mut rx_net_stream) => {
            result
        },
    }
}

async fn bind_udp_sockets(listen_addrs : &Vec<SocketAddr>, args : &NcArgs) -> std::io::Result<Vec<UdpTrafficSocket>> {
    // Map the listening addresses to a set of sockets, and bind them.
    let mut listening_sockets = vec![];
    for listen_addr in listen_addrs {
        let socket = tokio::net::UdpSocket::bind(*listen_addr).await?;
        configure_socket_options(&socket, listen_addr.is_ipv4(), args)?;

        listening_sockets.push(UdpTrafficSocket::new(socket));

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
        let sockets = bind_udp_sockets(&source_addrs, args).await?;
        assert!(sockets.len() == 1);

        return handle_udp_sockets(sockets, Some((0, addr)), args).await;
    }

    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Host not found."))
}

async fn do_udp_listen(listen_addrs : &Vec<SocketAddr>, args : &NcArgs) -> std::io::Result<()> {
    let listeners = bind_udp_sockets(&listen_addrs, args).await?;
    handle_udp_sockets(listeners, None, args).await
}

async fn handle_udp_sockets(mut sockets : Vec<UdpTrafficSocket>, first_peer : Option<(usize, SocketAddr)>, args : &NcArgs) -> std::io::Result<()> {
    fn print_udp_assoc(socket : &tokio::net::UdpSocket, peer_addr : &SocketAddr) {
        let local_addr = socket.local_addr().unwrap();
        eprintln!("Associating UDP socket from {} to {}, family {}",
            local_addr,
            peer_addr,
            if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" });
    }

    // Set up a thread to read from stdin. It will produce only chunks of the required datagram size to send.
    let mut stdin = setup_async_stdin_reader_thread(args.sendbuf_size, args.should_disable_console_char_mode);

    let mut stdout = tokio::io::stdout();

    // Track the list of known remote peers who have sent traffic to one of our listening sockets.
    let mut known_peers = std::collections::HashSet::<SocketAddr>::new();

    // Track the target peer to use for send operations. The caller might pass in a first address as a target.
    let mut next_target_peer : Option<(Arc<tokio::net::UdpSocket>, SocketAddr)> = first_peer.map(|(i, peer_addr)| {
        print_udp_assoc(&sockets[i].socket, &peer_addr);
        (sockets[i].socket.clone(), peer_addr)
    });

    loop {
        // Need to make a copy of it for use in this async operation, because it's set from from the recv path.
        let target_peer = next_target_peer.clone();
        let stdin_to_net = async {
            if let Some((tx_socket, peer_addr)) = target_peer {
                if let Some(Ok(b)) = stdin.next().await {
                    assert!(b.len() == args.sendbuf_size as usize);
                    tx_socket.send_to(&b, peer_addr).await?;
                    Ok(StdinStatus::StillOpen)
                } else {
                    Ok(StdinStatus::Closed)
                }
            } else {
                // If no destination peer is known, hang this async block, because there's nothing to do, nowhere to
                // send stdin traffic. After the net_to_stdout async block completes, a target peer will be known and
                // this one can start doing something useful.
                future::pending().await
            }
        };

        let net_to_stdout = async {
            // Listen for incoming UDP packets on all sockets at the same time. select_all waits for the first
            // recv_from to complete and cancels waiting on all the rest.
            let (listen_result, i, _) = futures::future::select_all(sockets.iter_mut().map(|listener| {
                Box::pin(listener.socket.recv_from(&mut listener.recv_buf))
            })).await;

            match listen_result {
                Ok((rx_bytes, peer_addr)) => {
                    let recv_buf = sockets[i].recv_buf;

                    // Set the destination peer address for stdin traffic to the first one we received data from.
                    if next_target_peer.is_none() {
                        print_udp_assoc(&sockets[i].socket, &peer_addr);
                        next_target_peer = Some((sockets[i].socket.clone(), peer_addr.clone()));
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
                        stdout.write_all(&recv_buf[.. rx_bytes]).await?;

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
pub struct NcArgs {
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

    /// Zero-IO mode. Only test for connection (TCP only)
    #[arg(short = 'z', conflicts_with = "is_udp")]
    is_zero_io : bool,

    /// Send broadcast data (UDP only)
    #[arg(short = 'b', requires = "is_udp")]
    is_broadcast : bool,

    /// Disable console character mode
    #[arg(short = 'c')]
    should_disable_console_char_mode : bool,

    /// Set Time-to-Live (hop count. IPv4 only)
    #[arg(short = 'i', long = "ttl")]
    ttl_opt : Option<u32>,

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
