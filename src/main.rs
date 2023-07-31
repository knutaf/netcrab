// For AsRawFd/AsRawSocket shenanigans.
#![feature(trait_alias)]

use clap::{Args, CommandFactory, Parser};
use futures::{channel::mpsc, future, SinkExt, StreamExt};
use rand::{distributions::Distribution, Rng};
use std::{
    io::{IsTerminal, Read, Write},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

#[macro_use]
extern crate lazy_static;

lazy_static! {
    // Make a static vector with all printable ASCII characters. If we wanted to avoid a static allocation we'd need to
    // implement a sibling of RandBytesIter that contains this vector with it, so that Slice could reference within the
    // object.
    static ref ASCII_CHARS_STORAGE : Vec<u8> = {
        (0u8..=u8::MAX).filter(|i| {
            let c = *i as char;
            c.is_ascii() && !c.is_control()
        }).collect()
    };
}

// A refcounted socket plus a recv buffer to accompany it, especially useful when listening for traffic on a collection
// of sockets at the same time, so each socket has its own associated receive buffer during the asynchronous operation.
struct UdpTrafficSocket {
    socket: Arc<tokio::net::UdpSocket>,
    recv_buf: [u8; 2000], // arbitrary size, bigger than a 1500 byte datagram
}

impl UdpTrafficSocket {
    fn new(socket: tokio::net::UdpSocket) -> UdpTrafficSocket {
        UdpTrafficSocket {
            socket: Arc::new(socket),
            recv_buf: [0; 2000],
        }
    }
}

// Represents the status of the input stream to the program, whether it's still open or has been closed.
enum LocalInputStatus {
    StillOpen,
    Closed,
}

// Convenience method for formatting an io::Error to String.
fn format_io_err(err: std::io::Error) -> String {
    format!("{}: {}", err.kind(), err)
}

// Tokio's async Stdin implementation doesn't work well for interactive uses. It uses a blocking read, so if we notice
// the socket (not stdin) close, we can't cancel the read on stdin. The user has to hit enter to make the read call
// finish, and then the next read won't be started.
//
// This isn't very nice for interactive uses. Instead, this thread uses a separate thread for the blocking stdin read.
// If the remote socket closes then this thread can be exited without having to wait for the async operation to finish.
fn setup_async_stdin_reader_thread(
    chunk_size: u32,
    input_mode: InputMode,
) -> mpsc::UnboundedReceiver<std::io::Result<bytes::Bytes>> {
    // Unbounded is OK because we'd rather prioritize faster throughput at the cost of more memory.
    let (mut tx_input, rx_input) = mpsc::unbounded();

    // If the user is interactively using the program, by default use "character mode", which inputs a character on
    // every keystroke rather than a full line when hitting enter. However, the user can request to use normal stdin
    // mode.
    let is_char_mode = std::io::stdin().is_terminal() && (input_mode != InputMode::StdinNoCharMode);

    std::thread::Builder::new()
        .name("stdin_reader".to_string())
        .spawn(move || {
            // Create storage for the input from stdin. It needs to be big enough to store the user's desired chunk size
            // It will accumulate data until it's filled an entire chunk, at which point it will be sent and repurposed
            // for the next chunk.
            let mut read_buf = vec![0u8; chunk_size as usize];

            // The available buffer (pointer to next byte to write and remaining available space in the chunk).
            let mut available_buf = &mut read_buf[..];

            if is_char_mode {
                // The console crate has a function called stdout that gives you, uh, a Term object that also services
                // input. OK dude.
                let mut stdio = console::Term::stdout();

                // A buffer specifically for encoding a single `char` read by the console crate.
                let mut char_buf = [0u8; std::mem::size_of::<char>()];
                let char_buf_len = char_buf.len();

                while let Ok(ch) = stdio.read_char() {
                    // Encode the char from input as a series of bytes.
                    let encoded_str = ch.encode_utf8(&mut char_buf);
                    let num_bytes_read = encoded_str.len();
                    assert!(num_bytes_read <= char_buf_len);

                    // Echo the char back out, because `read_char` doesn't.
                    let _ = stdio.write(encoded_str.as_bytes());

                    // Track a slice of the remaining bytes of the just-read char that haven't been sent yet.
                    let mut char_bytes_remaining = &mut char_buf[..num_bytes_read];
                    while char_bytes_remaining.len() > 0 {
                        // There might not be enough space left in the chunk to fit the whole char.
                        let num_bytes_copyable =
                            std::cmp::min(available_buf.len(), char_bytes_remaining.len());
                        assert!(num_bytes_copyable <= available_buf.len());
                        assert!(num_bytes_copyable <= char_bytes_remaining.len());
                        assert!(num_bytes_copyable > 0);

                        // Copy as much of the char as possible into the chunk. Note that for UTF-8 characters sent in
                        // UDP datagrams, if we fail to copy the whole character into a given datagram, the resultant
                        // traffic might be... strange. Imagine having a UTF-8 character split across two datagrams.
                        // Not great, but also not lossy, so who is to say what is correct? It's not possible to fully
                        // fill every UDP datagram of arbitrary size with varying size UTF-8 characters and not
                        // sometimes slice them.
                        available_buf[..num_bytes_copyable]
                            .copy_from_slice(&char_bytes_remaining[..num_bytes_copyable]);

                        // Update the available buffer to the remaining space in the chunk after copying in this char.
                        available_buf = &mut available_buf[num_bytes_copyable..];

                        // Advance the remaining slice of the char to the uncopied portion.
                        char_bytes_remaining = &mut char_bytes_remaining[num_bytes_copyable..];

                        // There's no more available buffer in this chunk, meaning we've accumulated a full chunk, so
                        // send it now.
                        if available_buf.len() == 0 {
                            // Stop borrowing read_buf for a hot second so it can be sent.
                            available_buf = &mut [];

                            if let Err(_) = tx_input
                                .unbounded_send(Ok(bytes::Bytes::copy_from_slice(&read_buf)))
                            {
                                break;
                            }

                            // The chunk was sent. Reset the available buffer to allow storing the next chunk.
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
                        if let Err(_) =
                            tx_input.unbounded_send(Ok(bytes::Bytes::copy_from_slice(&read_buf)))
                        {
                            break;
                        }

                        // The chunk was sent. Reset the buffer to allow storing the next chunk.
                        available_buf = &mut read_buf[..];
                    } else {
                        // Read buffer isn't full yet. Set the available buffer to the rest of the buffer just past the
                        // portion that was written to.
                        available_buf = &mut available_buf[num_bytes_read..];
                    }
                }
            }
        })
        .expect("Failed to create stdin reader thread");

    rx_input
}

// Set up a stream and sink that just echoes the stream back into the sink.
fn setup_echo_channel(
    args: &NcArgs,
) -> (
    Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>>>>,
    Pin<Box<dyn futures::Sink<bytes::Bytes, Error = std::io::Error>>>,
) {
    // Unbounded is OK because we'd rather prioritize faster throughput at the cost of more memory.
    let (tx_input, rx_input) = mpsc::unbounded();

    // Transform Bytes into Result<Bytes> and optionally also echo to stdout. Need to specify the boxed dynamic type
    // here because the match arms have different concrete types.
    let rx_input: Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>>>> =
        match args.output_mode {
            OutputMode::Stdout => {
                let mut stdout = std::io::stdout();
                Box::pin(
                    rx_input
                        .inspect(move |e: &bytes::Bytes| {
                            let _ = stdout.write_all(&e);
                            let _ = stdout.flush();
                        })
                        .map(Ok),
                )
            }
            OutputMode::Null => {
                // Transform Bytes into Result<Bytes>.
                Box::pin(rx_input.map(Ok))
            }
        };

    // Transform SendError into std::io::Error.
    let tx_input = Box::pin(tx_input.sink_map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            format!("Echo tx failed! {}", e),
        )
    }));

    (rx_input, tx_input)
}

// An iterator that generates random u8 values according to the given distribution. Actually wraps that in a
// Result<Bytes> to fit the way it is used to produce a stream of Bytes.
struct RandBytesIter<'a, TDist>
where
    TDist: rand::distributions::Distribution<u8>,
{
    args: &'a NcArgs,

    // TODO: Would rather make this a template type or impl that satisfies Iterator<Item = u8> but don't know how to
    // express that.
    rand_iter: rand::distributions::DistIter<TDist, rand::rngs::ThreadRng, u8>,
    rng: rand::rngs::ThreadRng,

    // Temporary storage for filling the next chunk before it's emitted from next(). Stored here to reduce allocations.
    chunk_storage: Vec<u8>,
}

impl<'a, TDist> RandBytesIter<'a, TDist>
where
    TDist: rand::distributions::Distribution<u8>,
{
    fn new(args: &'a NcArgs, dist: TDist) -> RandBytesIter<TDist> {
        RandBytesIter {
            args,
            rand_iter: rand::thread_rng().sample_iter(dist),
            rng: rand::thread_rng(),
            chunk_storage: vec![0u8; args.rand_config.size_max],
        }
    }
}

impl<'a, TDist> Iterator for RandBytesIter<'a, TDist>
where
    TDist: rand::distributions::Distribution<u8>,
{
    type Item = std::io::Result<bytes::Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        // Figure out the next random size of the chunk.
        let next_size = self
            .rng
            .gen_range(self.args.rand_config.size_min..=self.args.rand_config.size_max);

        // Fill that amount of the temporary storage with random data.
        for i in 0..next_size {
            self.chunk_storage[i] = self.rand_iter.next().unwrap();
        }

        // Generate a Bytes that contains that portion of the data. For now this is an allocation.
        // TODO: can we get rid of this allocation?
        Some(Ok(bytes::Bytes::copy_from_slice(
            &self.chunk_storage[0..next_size],
        )))
    }
}

fn setup_random_io(
    args: &NcArgs,
) -> (
    Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>> + '_>>,
    Pin<Box<dyn futures::Sink<bytes::Bytes, Error = std::io::Error>>>,
) {
    // Decide the type of data to be produced, depending on user choice. Have to specify the dynamic type here because
    // the match arms have different concrete types (different Distribution implementations).
    let rng_iter: Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>>>> =
        match args.rand_config.vals {
            RandValueType::Binary => {
                // Use a standard distribution across all u8 values.
                Box::pin(futures::stream::iter(RandBytesIter::new(
                    args,
                    rand::distributions::Standard,
                )))
            }
            RandValueType::Ascii => {
                // Make a distribution that references only ASCII characters. Slice returns a reference to the element in the
                // original array, so map and dereference to return u8 instead of &u8.
                let ascii_dist = rand::distributions::Slice::new(&ASCII_CHARS_STORAGE)
                    .unwrap()
                    .map(|e| *e);
                Box::pin(futures::stream::iter(RandBytesIter::new(args, ascii_dist)))
            }
        };

    (
        rng_iter,
        Box::pin(FramedWrite::new(tokio::io::stdout(), BytesCodec::new())),
    )
}

fn setup_local_io(
    sendbuf_size: u32,
    args: &NcArgs,
) -> (
    Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>> + '_>>,
    Pin<Box<dyn futures::Sink<bytes::Bytes, Error = std::io::Error>>>,
) {
    match args.input_mode {
        InputMode::Stdin | InputMode::StdinNoCharMode => {
            let codec = BytesCodec::new();

            // Setup an output sink that either goes to stdout or to the void, depending on the user's selection. Need
            // to specify the type here because the match arms have different concrete types.
            let output: Pin<Box<dyn futures::Sink<bytes::Bytes, Error = std::io::Error>>> =
                match args.output_mode {
                    OutputMode::Stdout => Box::pin(FramedWrite::new(tokio::io::stdout(), codec)),
                    OutputMode::Null => Box::pin(FramedWrite::new(tokio::io::sink(), codec)),
                };

            (
                // Set up a thread to read from stdin. It will produce only chunks of the required size to send.
                Box::pin(setup_async_stdin_reader_thread(
                    sendbuf_size,
                    args.input_mode,
                )),
                output,
            )
        }
        InputMode::Echo => setup_echo_channel(args),
        InputMode::Random => setup_random_io(args),
    }
}

// Send bytes from the local machine's input to the destination, and send incoming bytes to the local output. If any
// error occurs, bail out. It most likely means the remote side was closed. The local input ending is not an error.
async fn service_local_io_and_net<F1, F2>(
    local_input_to_net: F1,
    net_to_local_output: F2,
) -> Option<std::io::Result<()>>
where
    F1: futures::Future<Output = std::io::Result<LocalInputStatus>>,
    F2: futures::Future<Output = std::io::Result<()>>,
{
    tokio::select! {
        result = local_input_to_net => {
            match result {
                Ok(LocalInputStatus::StillOpen) => None,
                Ok(LocalInputStatus::Closed) => {
                    eprintln!("End of local input reached.");
                    Some(Ok(()))
                },

                // and() is needed to transform into this function's output result type
                Err(_) => Some(result.and(Ok(()))),
            }
        },
        result = net_to_local_output => {
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

    #[cfg(windows)]
    use std::os::windows::io::{AsRawSocket, FromRawSocket};

    #[cfg(unix)]
    use std::os::fd::{AsRawFd, FromRawFd};

    #[cfg(windows)]
    pub trait AsRawHandleType = AsRawSocket;

    #[cfg(unix)]
    pub trait AsRawHandleType = AsRawFd;

    // There isn't a clean way to get from a Tokio TcpSocket or UdpSocket to its inner socket2::Socket, which offers
    // basically all the possible setsockopts wrapped nicely. We can create a socket2::Socket out of a raw socket/fd
    // though. But the socket2::Socket takes ownership of the handle and will close it on dtor, so wrap it in
    // `ManuallyDrop`, which lets us leak it deliberately. The socket is owned properly by the Tokio socket anyway, so
    // nothing is leaked.
    //
    // The way to create a socket2::Socket out of a raw handle/fd is different on Windows vs Unix, so support both ways.
    // They only differ by whether RawSocket or RawFd is used.
    //
    // So as not to have to return the contents of the ManuallyDrop and to contain where this leaked socket goes, take
    // a closure so the caller can pass in what to do with the socket.
    //
    // SAFETY:
    // - the socket is a valid open socket at the point of this call.
    // - the socket can be closed by closesocket - this doesn't apply, since we won't be closing it here.
    pub fn with_socket2_from_socket<S, F>(socket: &S, func: F) -> std::io::Result<()>
    where
        S: AsRawHandleType,
        F: FnOnce(&socket2::Socket) -> std::io::Result<()>,
    {
        let socket2 = ManuallyDrop::new(unsafe {
            #[cfg(windows)]
            let s = socket2::Socket::from_raw_socket(socket.as_raw_socket());

            #[cfg(unix)]
            let s = socket2::Socket::from_raw_fd(socket.as_raw_fd());

            s
        });

        func(&socket2)
    }
}

fn configure_socket_options<S>(socket: &S, is_ipv4: bool, args: &NcArgs) -> std::io::Result<()>
where
    S: sockconf::AsRawHandleType,
{
    sockconf::with_socket2_from_socket(socket, |s2: &socket2::Socket| {
        let socktype = s2.r#type()?;

        match socktype {
            socket2::Type::DGRAM => {
                s2.set_broadcast(args.is_broadcast)?;

                let multicast_ttl = args.ttl_opt.unwrap_or(1);
                if is_ipv4 {
                    s2.set_multicast_loop_v4(!args.should_disable_multicast_loopback)?;
                    s2.set_multicast_ttl_v4(multicast_ttl)?;
                } else {
                    s2.set_multicast_loop_v6(!args.should_disable_multicast_loopback)?;
                    s2.set_multicast_hops_v6(multicast_ttl)?;
                }
            }
            socket2::Type::STREAM => {
                // No stream-specific options for now
            }
            _ => {
                eprintln!("Warning: unknown socket type {:?}", socktype);
            }
        }

        s2.set_send_buffer_size(args.sendbuf_size as usize)?;

        // If joining a multicast group, the TTL param was used above for the multicast TTL. Don't set it as the unicast
        // TTL too.
        if !args.should_join_multicast_group {
            if let Some(ttl) = args.ttl_opt {
                if is_ipv4 {
                    s2.set_ttl(ttl)?;
                } else {
                    s2.set_unicast_hops_v6(ttl)?;
                }
            }
        }

        Ok(())
    })
}

fn get_interface_index_from_local_addr(local_addr: &SocketAddr) -> std::io::Result<u32> {
    // The unspecified address won't show up in an enumeration of the machine's interfaces, but is represented
    // by interface 0.
    match local_addr {
        SocketAddr::V4(v4) => {
            if *v4.ip() == std::net::Ipv4Addr::UNSPECIFIED {
                return Ok(0);
            }
        }
        SocketAddr::V6(v6) => {
            if *v6.ip() == std::net::Ipv6Addr::UNSPECIFIED {
                return Ok(0);
            }
        }
    };

    let interfaces = default_net::get_interfaces();
    match local_addr {
        SocketAddr::V4(v4) => {
            let ip = v4.ip();
            interfaces.iter().find(|interface| {
                interface
                    .ipv4
                    .iter()
                    .any(|if_v4_addr| if_v4_addr.addr == *ip)
            })
        }
        SocketAddr::V6(v6) => {
            let ip = v6.ip();
            interfaces.iter().find(|interface| {
                interface
                    .ipv6
                    .iter()
                    .any(|if_v6_addr| if_v6_addr.addr == *ip)
            })
        }
    }
    .map(|interface| interface.index)
    .ok_or(std::io::Error::new(
        std::io::ErrorKind::AddrNotAvailable,
        "Could not find local interface matching local address.",
    ))
}

fn join_multicast_group<S>(
    socket: &S,
    local_addr: &SocketAddr,
    multi_addr: &SocketAddr,
) -> std::io::Result<()>
where
    S: sockconf::AsRawHandleType,
{
    sockconf::with_socket2_from_socket(socket, |s2: &socket2::Socket| {
        let interface_index = get_interface_index_from_local_addr(local_addr)?;

        match multi_addr {
            SocketAddr::V4(addr) => s2.join_multicast_v4_n(
                addr.ip(),
                &socket2::InterfaceIndexOrAddress::Index(interface_index),
            ),
            SocketAddr::V6(addr) => s2.join_multicast_v6(addr.ip(), interface_index),
        }
    })
}

async fn do_tcp_connect(
    hostname: &str,
    port: u16,
    source_addrs: &Vec<SocketAddr>,
    args: &NcArgs,
) -> std::io::Result<()> {
    assert!(!args.af_limit.use_v4 || !args.af_limit.use_v6);

    let candidates = tokio::net::lookup_host(format!("{}:{}", hostname, port)).await?;

    let mut candidate_count = 0;
    for addr in candidates {
        // Skip incompatible candidates from what address family the user specified.
        if args.af_limit.use_v4 && addr.is_ipv6() || args.af_limit.use_v6 && addr.is_ipv4() {
            continue;
        }

        candidate_count += 1;

        match tcp_connect_to_candidate(addr, source_addrs, args).await {
            Ok(tcp_stream) => {
                if args.is_zero_io {
                    return Ok(());
                } else {
                    // Return after first successful connection.
                    return handle_tcp_stream(tcp_stream, args).await;
                }
            }
            Err(e) => {
                eprintln!("Failed to connect to {}. Error: {}", addr, e);
            }
        }
    }

    if candidate_count == 0 {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Host not found.",
        ))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "Failed to connect to all candidates.",
        ))
    }
}

async fn tcp_connect_to_candidate(
    addr: SocketAddr,
    source_addrs: &Vec<SocketAddr>,
    args: &NcArgs,
) -> std::io::Result<tokio::net::TcpStream> {
    eprintln!("Connecting to {}", addr);

    let socket = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()
    } else {
        tokio::net::TcpSocket::new_v6()
    }?;

    configure_socket_options(&socket, addr.is_ipv4(), args)?;

    // Bind the local socket to the first local address that matches the address family of the destination.
    let source_addr = source_addrs
        .iter()
        .find(|e| e.is_ipv4() == addr.is_ipv4())
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No matching local address matched destination host's address family",
        ))?;
    socket.bind(*source_addr)?;

    let stream = socket.connect(addr).await?;

    let local_addr = stream.local_addr()?;
    let peer_addr = stream.peer_addr()?;
    eprintln!(
        "Connected from {} to {}, protocol TCP, family {}",
        local_addr,
        peer_addr,
        if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" }
    );

    Ok(stream)
}

fn get_local_addrs(
    local_host_opt: Option<&str>,
    local_port: u16,
    af_limit: &AfLimit,
) -> std::io::Result<Vec<SocketAddr>> {
    assert!(!af_limit.use_v4 || !af_limit.use_v6);

    // If the caller specified a specific address, include that. Otherwise, include all unspecified addresses.
    let mut addrs = if let Some(local_host) = local_host_opt {
        vec![format!("{}:{}", local_host, local_port)
            .parse()
            .or(Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)))?]
    } else {
        vec![
            SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::UNSPECIFIED,
                local_port,
            )),
            SocketAddr::V6(std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::UNSPECIFIED,
                local_port,
                0,
                0,
            )),
        ]
    };

    // If the caller specified only one address family, filter out any incompatible address families.
    let addrs = addrs
        .drain(..)
        .filter(|e| !(af_limit.use_v4 && e.is_ipv6() || af_limit.use_v6 && e.is_ipv4()))
        .collect();

    Ok(addrs)
}

async fn do_tcp_listen(listen_addrs: &Vec<SocketAddr>, args: &NcArgs) -> std::io::Result<()> {
    // Map the listening addresses to a set of sockets, and bind them.
    let mut listening_sockets = vec![];
    for listen_addr in listen_addrs {
        let listening_socket = if listen_addr.is_ipv4() {
            tokio::net::TcpSocket::new_v4()
        } else {
            tokio::net::TcpSocket::new_v6()
        }?;

        configure_socket_options(&listening_socket, listen_addr.is_ipv4(), args)?;
        listening_socket.bind(*listen_addr)?;
        listening_sockets.push(listening_socket);
    }

    // Listen on all the sockets.
    let mut listeners = vec![];
    for listening_socket in listening_sockets {
        let local_addr = listening_socket.local_addr()?;
        eprintln!(
            "Listening on {}, protocol TCP, family {}",
            local_addr,
            if local_addr.is_ipv4() { "IPv4" } else { "IPv6" }
        );

        listeners.push(listening_socket.listen(1)?);
    }

    loop {
        // Try to accept connections on any of the listening sockets. Handle clients one at a time. select_all waits for
        // the first accept to complete and cancels waiting on all the rest.
        let (listen_result, _i, _rest) = futures::future::select_all(
            listeners.iter().map(|listener| Box::pin(listener.accept())),
        )
        .await;

        match listen_result {
            Ok((stream, ref peer_addr)) => {
                eprintln!(
                    "Accepted connection from {}, protocol TCP, family {}",
                    peer_addr,
                    if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" }
                );

                // In Zero-IO mode, immediately close the socket. Otherwise, handle it like normal.
                let stream_result = if args.is_zero_io {
                    Ok(())
                } else {
                    handle_tcp_stream(stream, args).await
                };

                // After handling a client, either loop and accept another client or exit.
                if !args.is_listening_repeatedly {
                    return stream_result;
                } else {
                    match stream_result {
                        Ok(_) => eprintln!("Connection from {} closed gracefully.", peer_addr),
                        Err(e) => {
                            eprintln!("Connection from {} closed with result: {}", peer_addr, e)
                        }
                    }
                }
            }
            Err(e) => {
                if !args.is_listening_repeatedly {
                    return Err(e);
                } else {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

async fn handle_tcp_stream(
    mut tcp_stream: tokio::net::TcpStream,
    args: &NcArgs,
) -> std::io::Result<()> {
    let (rx_socket, tx_socket) = tcp_stream.split();

    let (mut data_from_local, mut data_to_local) = setup_local_io(1, args);

    // Set up a sink that ends up sending out of the TCP socket. Local data can be streamed straight into it.
    let mut local_to_net_sink = FramedWrite::new(tx_socket, BytesCodec::new());

    // Set up a stream that produces chunks of data from the network. Filter map the Result<BytesMut, Error> stream into
    // just a Bytes stream to match the data_to_local Sink. On Error, log the error and end the stream.
    let mut net_to_local_stream =
        FramedRead::new(rx_socket, BytesCodec::new()).filter_map(|bm| match bm {
            //BytesMut into Bytes
            Ok(bm) => future::ready(Some(Ok(bm.freeze()))),
            Err(e) => {
                eprintln!("failed to read from socket; error={}", e);
                future::ready(None)
            }
        });

    // End when either the socket is closed or the local IO is closed.
    tokio::select! {
        result = local_to_net_sink.send_all(&mut data_from_local) => {
            eprintln!("End of outbound data from local machine reached.");
            result
        },
        result = data_to_local.send_all(&mut net_to_local_stream) => {
            result
        },
    }
}

async fn bind_udp_sockets(
    listen_addrs: &Vec<SocketAddr>,
    args: &NcArgs,
) -> std::io::Result<Vec<UdpTrafficSocket>> {
    // Map the listening addresses to a set of sockets, and bind them.
    let mut listening_sockets = vec![];
    for listen_addr in listen_addrs {
        let socket = tokio::net::UdpSocket::bind(*listen_addr).await?;
        configure_socket_options(&socket, listen_addr.is_ipv4(), args)?;

        listening_sockets.push(UdpTrafficSocket::new(socket));

        eprintln!(
            "Bound UDP socket to {}, family {}",
            listen_addr,
            if listen_addr.is_ipv4() {
                "IPv4"
            } else {
                "IPv6"
            }
        );
    }

    if listening_sockets.len() == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "Could not bind any socket.",
        ));
    }

    Ok(listening_sockets)
}

async fn do_udp_connection(
    hostname: &str,
    port: u16,
    source_addrs: &Vec<SocketAddr>,
    args: &NcArgs,
) -> std::io::Result<()> {
    assert!(!args.af_limit.use_v4 || !args.af_limit.use_v6);

    let candidates = tokio::net::lookup_host(format!("{}:{}", hostname, port)).await?;

    // Only use the first candidate. For UDP the concept of a "connection" is limited to being the implicit recipient of
    // a `send()` call. UDP also can't determine success, because the recipient might properly receive the packet but
    // choose not to indicate any response.
    for addr in candidates {
        // Skip incompatible candidates from what address family the user specified.
        if args.af_limit.use_v4 && addr.is_ipv6() || args.af_limit.use_v6 && addr.is_ipv4() {
            continue;
        }

        // Filter the source address list to exactly one, which matches the address family of the destination.
        let source_addrs: Vec<SocketAddr> = source_addrs
            .iter()
            .filter_map(|e| {
                if e.is_ipv4() == addr.is_ipv4() {
                    Some(*e)
                } else {
                    None
                }
            })
            .take(1)
            .collect();
        assert!(source_addrs.len() == 1);

        // Bind to a single socket that matches the address family of the target address.
        let sockets = bind_udp_sockets(&source_addrs, args).await?;
        assert!(sockets.len() == 1);

        if args.should_join_multicast_group {
            join_multicast_group(&*(sockets[0].socket), &source_addrs[0], &addr)?;
        }

        return handle_udp_sockets(sockets, Some((0, addr)), args).await;
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "Host not found.",
    ))
}

async fn do_udp_listen(listen_addrs: &Vec<SocketAddr>, args: &NcArgs) -> std::io::Result<()> {
    let listeners = bind_udp_sockets(&listen_addrs, args).await?;
    handle_udp_sockets(listeners, None, args).await
}

async fn handle_udp_sockets(
    mut sockets: Vec<UdpTrafficSocket>,
    first_peer: Option<(usize, SocketAddr)>,
    args: &NcArgs,
) -> std::io::Result<()> {
    fn print_udp_assoc(socket: &tokio::net::UdpSocket, peer_addr: &SocketAddr) {
        let local_addr = socket.local_addr().unwrap();
        eprintln!(
            "Associating UDP socket from {} to {}, family {}",
            local_addr,
            peer_addr,
            if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" }
        );
    }

    let (mut data_from_local, mut data_to_local) = setup_local_io(args.sendbuf_size, args);

    // Track the list of known remote peers who have sent traffic to one of our listening sockets.
    let mut known_peers = std::collections::HashSet::<SocketAddr>::new();

    // Track the target peer to use for send operations. The caller might pass in a first address as a target.
    let mut next_target_peer: Option<(Arc<tokio::net::UdpSocket>, SocketAddr)> =
        first_peer.map(|(i, peer_addr)| {
            print_udp_assoc(&sockets[i].socket, &peer_addr);
            (sockets[i].socket.clone(), peer_addr)
        });

    loop {
        // Need to make a copy of it for use in this async operation, because it's set from from the recv path.
        let target_peer = next_target_peer.clone();
        let local_to_net = async {
            if let Some((tx_socket, peer_addr)) = target_peer {
                if let Some(Ok(b)) = data_from_local.next().await {
                    tx_socket.send_to(&b, peer_addr).await?;
                    Ok(LocalInputStatus::StillOpen)
                } else {
                    Ok(LocalInputStatus::Closed)
                }
            } else {
                // If no destination peer is known, hang this async block, because there's nothing to do, nowhere to
                // send outgoing traffic. After the net_to_local async block completes, a target peer will be known and
                // this one can start doing something useful.
                future::pending().await
            }
        };

        let net_to_local = async {
            // Listen for incoming UDP packets on all sockets at the same time. select_all waits for the first
            // recv_from to complete and cancels waiting on all the rest.
            let (listen_result, i, _) = futures::future::select_all(
                sockets
                    .iter_mut()
                    .map(|listener| Box::pin(listener.socket.recv_from(&mut listener.recv_buf))),
            )
            .await;

            match listen_result {
                Ok((rx_bytes, peer_addr)) => {
                    let recv_buf = &sockets[i].recv_buf;

                    // Set the destination peer address for outbound traffic to the first one we received data from.
                    if next_target_peer.is_none() {
                        print_udp_assoc(&sockets[i].socket, &peer_addr);
                        next_target_peer = Some((sockets[i].socket.clone(), peer_addr.clone()));
                    }

                    // Only print out the first time receiving a datagram from a given peer.
                    if known_peers.insert(peer_addr.clone()) {
                        eprintln!(
                            "Received UDP datagram from {}, family {}",
                            peer_addr,
                            if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" }
                        );
                    }

                    // If the entire buffer filled up (which is bigger than a normal MTU) then whatever is happening is
                    // unsupported.
                    if rx_bytes == recv_buf.len() {
                        eprintln!("Dropping datagram with unsupported size, larger than 2KB!");
                        Ok(())
                    } else {
                        // Send to local only the portion of the receive buffer that was filled by the datagram's
                        // payload. Have to copy here because we need to pass it to the sink, and then the receive path
                        // reuses the buffer for getting the next datagram.
                        data_to_local
                            .send(bytes::Bytes::copy_from_slice(&recv_buf[..rx_bytes]))
                            .await?;

                        data_to_local.flush().await?;
                        Ok(())
                    }
                }
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
                }
            }
        };

        match service_local_io_and_net(local_to_net, net_to_local).await {
            Some(res) => return res,
            None => {}
        }
    }
}

#[derive(Args, Clone)]
#[group(required = false, multiple = false)]
struct AfLimit {
    /// Use IPv4 only
    #[arg(short = '4')]
    use_v4: bool,

    /// Use IPv6 only
    #[arg(short = '6')]
    use_v6: bool,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
enum InputMode {
    /// Read from stdin
    Stdin,

    /// Stdin with character mode disabled
    #[value(name = "stdin-nochar", alias = "snc")]
    StdinNoCharMode,

    /// Echo inbound packets back to network
    #[value(name = "echo", alias = "e")]
    Echo,

    /// Generate random data to send to the network
    #[value(name = "rand", alias = "r")]
    Random,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
enum OutputMode {
    /// Output to stdout
    Stdout,

    /// No output
    #[value(name = "none", alias = "no")]
    Null,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
enum RandValueType {
    /// Binary data
    #[value(name = "binary", alias = "b")]
    Binary,

    /// ASCII data
    #[value(name = "ascii", alias = "a")]
    Ascii,
}

#[derive(Args, Clone)]
#[group(required = false, multiple = true)]
struct RandConfig {
    /// Min size for random sends
    #[arg(long = "rsizemin", default_value_t = 1)]
    size_min: usize,

    /// Max size for random sends
    #[arg(long = "rsizemax", default_value_t = 1450)]
    size_max: usize,

    /// Random value selection
    #[arg(long = "rvals", value_enum, default_value_t = RandValueType::Binary)]
    vals: RandValueType,
}

#[derive(clap::Parser, Clone)]
#[command(author, version, about, long_about = None, disable_help_flag = true, override_usage =
r#"connect outbound: nc [options] hostname port
       listen for inbound: nc [-l | -L] -p port [options]"#)]
pub struct NcArgs {
    /// this cruft (--help for long help)
    #[arg(short = 'h')]
    help: bool,

    #[arg(long = "help", hide = true)]
    help_more: bool,

    /// Use UDP instead of TCP
    #[arg(short = 'u')]
    is_udp: bool,

    /// Listen for incoming connections
    #[arg(short = 'l')]
    is_listening: bool,

    /// Listen repeatedly for incoming connections (implies -l)
    #[arg(short = 'L')]
    is_listening_repeatedly: bool,

    /// Source address to bind to
    #[arg(short = 's')]
    source_host: Option<String>,

    // Unspecified local port uses port 0, which when bound to assigns from the ephemeral port range.
    /// Port to bind to
    #[arg(short = 'p', default_value_t = 0)]
    source_port: u16,

    /// Send buffer/datagram size
    #[arg(long = "sb", default_value_t = 1)]
    sendbuf_size: u32,

    /// Zero-IO mode. Only test for connection (TCP only)
    #[arg(short = 'z', conflicts_with = "is_udp")]
    is_zero_io: bool,

    /// Send broadcast data (UDP only)
    #[arg(short = 'b', requires = "is_udp")]
    is_broadcast: bool,

    /// Set Time-to-Live
    #[arg(long = "ttl", value_name = "TTL")]
    ttl_opt: Option<u32>,

    /// Input mode
    #[arg(short = 'i', value_enum, default_value_t = InputMode::Stdin)]
    input_mode: InputMode,

    /// Output mode
    #[arg(short = 'o', value_name = "OUTPUT_MODE")]
    output_mode_opt: Option<OutputMode>,

    #[arg(long = "override_output_mode", hide = true, value_enum, default_value_t = OutputMode::Stdout)]
    output_mode: OutputMode,

    /// Join multicast group given by hostname (outbound UDP only)
    #[arg(
        long = "mc",
        requires = "is_udp",
        requires = "hostname",
        requires = "port"
    )]
    should_join_multicast_group: bool,

    /// Disable multicast sockets seeing their own traffic
    #[arg(
        long = "mc_no_loop",
        requires = "should_join_multicast_group",
        default_value_t = false
    )]
    should_disable_multicast_loopback: bool,

    /// Hostname to connect to
    #[arg(
        requires = "port",
        conflicts_with = "is_listening",
        conflicts_with = "is_listening_repeatedly"
    )]
    hostname: Option<String>,

    /// Port number to connect on
    #[arg(
        requires = "hostname",
        conflicts_with = "is_listening",
        conflicts_with = "is_listening_repeatedly"
    )]
    port: Option<u16>,

    #[command(flatten)]
    af_limit: AfLimit,

    #[command(flatten)]
    rand_config: RandConfig,
}

fn usage(msg: &str) -> ! {
    eprintln!("Error: {}", msg);
    eprintln!();
    let _ = NcArgs::command().print_help();
    std::process::exit(1)
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let mut args = NcArgs::parse();

    if args.help_more {
        let _ = NcArgs::command().print_long_help();
        std::process::exit(1)
    }

    if args.is_listening_repeatedly {
        args.is_listening = true;
    }

    // If output mode is not explicitly specified, assign it based on input mode.
    args.output_mode = if let Some(mode) = args.output_mode_opt {
        mode
    } else {
        match args.input_mode {
            InputMode::Stdin | InputMode::StdinNoCharMode | InputMode::Random => OutputMode::Stdout,
            InputMode::Echo => OutputMode::Null,
        }
    };

    // When joining a multicast group, by default you will send traffic to the group but won't receive it unless also
    // bound to the port you're sending to. If the user didn't explicitly choose a local port to bind to, choose the
    // outbound multicast port because it's probably what they actually wanted.
    if args.should_join_multicast_group && args.source_port == 0 {
        args.source_port = args.port.unwrap();
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
