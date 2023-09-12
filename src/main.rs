// For AsRawFd/AsRawSocket shenanigans.
#![feature(trait_alias)]

use bytes::Bytes;
use clap::{Args, CommandFactory, Parser, ValueEnum};
use futures::{channel::mpsc, future, stream::FuturesUnordered, FutureExt, SinkExt, StreamExt};
use rand::{distributions::Distribution, Rng};
use std::{
    collections::HashMap,
    io::{IsTerminal, Read, Write},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

// Some useful general notes about Rust async progarmming that might help when reading this code here.
//
// Many parts of the program use futures. A future is an object that contains some processing to defer eventually. It
// can be passed around and stored, and when we want to retrieve the value it will produce, we call `await`. Another
// way to get that value is to pass the future to a function like `select`.
//
// When we call `await` and the value isn't ready yet, the Tokio runtime switches to processing other tasks and will
// wake up when the value is ready.
//
// `select` lets you wait on multiple different types of futures at the same time and handle each type's completion with
// different code.
//
// This code heavily uses Sinks and Streams.
//
// A Sink is an object that accepts data via the `send` or `send_all` call. The generic type of the sink indicates what
// data type must be supplied to the Sink, and we can use the `with` method to change what type it accepts, like an
// adapter.
//
// A Stream is an object that *asynchronously* produces bytes, the asynchronous analog of an Iterator. You can pull data
// out of it using the `next` call or by passing it to a Sink's `send_all` call. Like an iterator, you can call `map` or
// various other methods to change the data type that is produced by the Stream.

// Bytes sourced from the local machine are marked with a special peer address so they can be treated similarly to
// sockets even when it's not a real socket (and therefore doesn't have a real network address.
const LOCAL_IO_PEER_ADDR: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(
    std::net::Ipv4Addr::UNSPECIFIED,
    0,
));

// A buffer of bytes that also carries the remote peer that sent it to this machine. Used for figuring out which remote
// machines it should be forwarded to.
#[derive(Debug)]
struct SourcedBytes {
    data: Bytes,
    peer: SocketAddr,
}

impl SourcedBytes {
    // Wrap bytes that were produced by the local machine with the special local peer address that marks them as
    // originating from the local machine. As a convenience, also wrap in a Result, which is what the various streams
    // and sinks need.
    fn ok_from_local(data: Bytes) -> std::io::Result<Self> {
        Ok(Self {
            data,
            peer: LOCAL_IO_PEER_ADDR,
        })
    }
}

type SockAddrSet = std::collections::HashSet<SocketAddr>;

// A grouping of a data buffer plus the address it should be sent to. Primarly used for UDP traffic, where the
// `UdpFramed` sink and stream use this tuple.
type TargetedBytes = (Bytes, SocketAddr);

// A stream of bytes produced from the local machine. In contrast with bytes that come from the network, it has no
// source address, though that is faked later in order to make it be treated just like other sockets by the router for
// purposes of forwarding.
//
// Lifetime specifier is needed because in some places the local stream incorporates an object that references function
// parameters (i.e. '_).
type LocalIoStream<'a> = Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + 'a>>;

// A sink that accepts byte buffers and sends them to the local IO function (stdout, echo, null, etc.).
type LocalIoSink = Pin<Box<dyn futures::Sink<Bytes, Error = std::io::Error>>>;

// When setting up local IO, it's common to set up both the way input enters the program and where output from the
// program should go.
type LocalIoSinkAndStream<'a> = (LocalIoSink, LocalIoStream<'a>);

// A sink that accepts TargetedBytes -- a byte buffer plus the destination it should go to. Note that for TCP sockets,
// the destination is ignored, because each socket already implicitly contains the destination.
//
// When the router determines that data should be sent to a socket, it sends it into this sink, where there is one per
// remote peer.
type RouterToNetSink = Pin<Box<dyn futures::Sink<TargetedBytes, Error = std::io::Error>>>;

// A stream of bytes plus the destination it should go to, produced by the router and consumed by a socket. The socket
// uses `send_all` to drive data from this stream into its per-socket sink and thus out to the network.
type RouterToNetStream = Pin<Box<dyn futures::Stream<Item = std::io::Result<TargetedBytes>>>>;

// A sink of bytes originating from some remote peer. Each socket drives data received on it to the router using this
// sink, supplying where the data came from. The router uses the data's origin to decide where the data should be
// forwarded to.
type NetToRouterSink = Pin<Box<dyn futures::Sink<SourcedBytes, Error = std::io::Error>>>;

// When the program wants to add a new remote peer to the router so that the router can forward data to it, the router
// provides a sink for the socket to send data into to reach the router; and a stream of data from the router that the
// socket should send to the remote peer.
type RouteSinkAndStream = (NetToRouterSink, RouterToNetStream);

#[derive(Debug)]
struct ConnectionTarget {
    addr_string: String,
    addrs: Vec<SocketAddr>,
}

impl ConnectionTarget {
    async fn new(addr_string: &str) -> std::io::Result<Self> {
        Ok(Self {
            addr_string: String::from(addr_string),
            addrs: tokio::net::lookup_host(&addr_string).await?.collect(),
        })
    }
}

impl std::fmt::Display for ConnectionTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", &self.addr_string)?;
        for addr in self.addrs.iter() {
            writeln!(f, "    {}", addr)?;
        }

        Ok(())
    }
}


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

// Core logic used by the router to decide where to forward messages.
fn should_forward_to(args: &NcArgs, source: &SocketAddr, dest: &SocketAddr) -> bool {
    // In echo mode, allow sending data back to the source.
    if source == dest && args.input_mode != InputMode::Echo {
        if args.verbose {
            eprintln!("Skipping forwarding back to source.");
        }

        return false;
    }

    // When brokering, any incoming traffic should be sent back to all other peers.
    // When not brokering, only send to the local output or back to the source (only possible in
    // echo mode).
    if !args.should_broker
        && (dest != source)
        && *dest != LOCAL_IO_PEER_ADDR
        && *source != LOCAL_IO_PEER_ADDR
    {
        if args.verbose {
            eprintln!("Skipping forwarding to other remote endpoint due to not broker mode.");
        }

        return false;
    }

    true
}

// An object that manages a set of known peers. It accepts data from one or more sockets and forwards incoming data
// either just to the local output or to other known peers, depending on the `should_broker` flag. The reason it is
// specific to TCP is because the sockets that it manages have the destination address embedded in them, unlike UDP,
// where a single socket sends to multiple destinations.
struct TcpRouter<'a> {
    args: &'a NcArgs,

    // A collection of known remote peers, indexed by the remote peer address. Each known peer has a sink that is used
    // for the router to send data to that peer.
    routes: HashMap<SocketAddr, RouterToNetSink>,

    // A sink where all sockets send data to the router for forwarding. Normally this would just be an UnboundedSender,
    // but since we map the send error, it gets stored as a complicated type. Thanks, Rust.
    net_collector_sink: futures::sink::SinkMapErr<
        mpsc::UnboundedSender<SourcedBytes>,
        fn(mpsc::SendError) -> std::io::Error,
    >,

    // An internal stream that produces all of the data from all sockets that come into the router.
    inbound_net_traffic_stream: mpsc::UnboundedReceiver<SourcedBytes>,

    // A stream of data produced from input to the program.
    local_io_stream: LocalIoStream<'a>,

    // A sink where the router can send data to be printed out.
    local_io_sink: LocalIoSink,

    // Used to figure out when to hook up the local input.
    lifetime_client_count: u32,
}

impl<'a> TcpRouter<'a> {
    pub fn new(args: &'a NcArgs) -> TcpRouter<'a> {
        let (net_collector_sink, inbound_net_traffic_stream) = mpsc::unbounded();

        // This funny syntax is required to coerce a function pointer to the fn type required by the field on the struct
        let net_collector_sink = net_collector_sink.sink_map_err(
            map_unbounded_sink_err_to_io_err as fn(mpsc::SendError) -> std::io::Error,
        );

        Self {
            args,
            routes: HashMap::new(),
            net_collector_sink,
            inbound_net_traffic_stream,
            local_io_stream: Box::pin(futures::stream::pending()),
            local_io_sink: Box::pin(
                futures::sink::drain().sink_map_err(map_drain_sink_err_to_io_err),
            ),
            lifetime_client_count: 0,
        }
    }

    // Callers use this to add a new destination to the router for forwarding. It provides back a sink that the caller
    // can use to pass in data from the network, and a stream of data that should be sent to the destination.
    pub fn add_route(&mut self, route_addr: SocketAddr) -> RouteSinkAndStream {
        // TODO: It would be great to take the socket sink directly and return the router sink, and eliminate this
        // internal channel, but that makes me take the TcpStream internally, and I can't figure out how to make the
        // lifetimes work.
        let (collector_to_net_sink, router_to_net_stream) = mpsc::unbounded();

        let collector_to_net_sink =
            collector_to_net_sink.sink_map_err(map_unbounded_sink_err_to_io_err);
        let router_to_net_stream = router_to_net_stream.map(Ok);

        // Store the sink where the router will send data that should go to this destination. The output end of the
        // stream is given back to the caller so they can pull data from it and send it to the actual socket.
        self.routes
            .insert(route_addr, Box::pin(collector_to_net_sink));

        // Don't add the local IO hookup until the first client is added, otherwise the router will pull all the data
        // out of, say, a redirected input stream, and forward it to nobody, because there are no other clients.
        self.lifetime_client_count += 1;
        if self.lifetime_client_count == 1 {
            (self.local_io_sink, self.local_io_stream) = setup_local_io(self.args);
        }

        // The input end of the router (`net_collector_sink`) can be cloned to allow multiple callers to pass data into
        // the same channel.
        (
            Box::pin(self.net_collector_sink.clone()),
            Box::pin(router_to_net_stream),
        )
    }

    // Start asynchronously processing data from all managed sockets.
    pub async fn service(&mut self) -> std::io::Result<()> {
        // Since there is only one local input and output (i.e. stdin/stdout), don't create a new channel to add it to
        // the router. Instead just feed data into the router directly by tagging it as originating from the local
        // input.
        let mut local_io_to_router_sink = Box::pin(
            self.net_collector_sink
                .clone()
                .with(|b| async { SourcedBytes::ok_from_local(b) }),
        );

        // If the local output (i.e. stdout) fails, we can set this to None to save perf on sending to it further.
        let mut local_io_sink_opt = Some(&mut self.local_io_sink);

        // This is servicing more than one type of event. Whenever one event type completes, after we handle it, loop
        // back and continue processing the rest of them. While one event is being serviced, the other ones are canceled
        // and then restarted.
        //
        // Well, I haven't done a thorough check as to the guarantees each future has around cancelation and
        // restarting, but eh it seems to work OK.
        //
        // There's another loop just like this in `handle_udp_sockets`.
        loop {
            futures::select! {
                // Service all incoming traffic from all sockets.
                sb = self.inbound_net_traffic_stream.select_next_some() => {
                    if self.args.verbose {
                        eprintln!("Router handling traffic: {:?}", sb);
                    }

                    // Broadcast any incoming data back to whichever other connected sockets and/or local IO it should
                    // go to. Track any failed sends so they can be pruned from the list of known routes after.
                    let mut broken_routes = vec![];
                    for (dest_addr, dest_sink) in self.routes.iter_mut() {
                        if !should_forward_to(self.args, &sb.peer, dest_addr) {
                            continue;
                        }

                        if self.args.verbose {
                            eprintln!("Forwarding to {}", dest_addr);
                        }

                        // It could be possible to omit the peer address for the TcpRouter, because the peer address is
                        // implicit with the socket, but I'm going to keep it this way for symmetry for now, until it
                        // becomes a perf problem.
                        if let Err(e) = dest_sink.send((sb.data.clone(), *dest_addr)).await {
                            eprintln!("Error forwarding to {}. {}", dest_addr, e);
                            broken_routes.push(*dest_addr);
                        }
                    }

                    // Came from a remote endpoint, so also send to local output if it hasn't failed yet.
                    if sb.peer != LOCAL_IO_PEER_ADDR {
                        if let Some(ref mut local_io_sink) = local_io_sink_opt {
                            // If we hit an error emitting output, clear out the local output sink so we don't bother
                            // trying to output more.
                            if let Err(e) = local_io_sink.send(sb.data.clone()).await {
                                eprintln!("Local output closed. {}", e);
                                local_io_sink_opt = None;
                            }
                        }
                    }

                    // If there were any failed sends, clear them out so we don't try to send to them in the future,
                    // which would just result in more errors.
                    if !broken_routes.is_empty() {
                        for pa in broken_routes.iter() {
                            self.routes.remove(pa).unwrap();
                        }
                    }
                },

                // Send in all data from the local input to the router.
                _result = local_io_to_router_sink.send_all(&mut self.local_io_stream).fuse() => {
                    eprintln!("End of outbound data from local machine reached.");
                    self.local_io_stream = Box::pin(futures::stream::pending());
                },
            }
        }
    }
}

// Convenience method for formatting an io::Error to String.
fn format_io_err(err: std::io::Error) -> String {
    format!("{}: {}", err.kind(), err)
}

// When sending data from a socket to the local output, remove the destination, to match the format that LocalIoSink
// requires.
async fn fut_remove_target_addr(input: TargetedBytes) -> std::io::Result<Bytes> {
    Ok(input.0)
}

// All mpsc::UnboundedSenders use a different error type than the std::io::Error that we use everywhere else in the
// program. This function makes the conversion.
fn map_unbounded_sink_err_to_io_err(err: mpsc::SendError) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        format!("UnboundedSender tx failed! {}", err),
    )
}

// In a few places we temporarily use a Drain, which never fails, but we need to switch the error type to match what is
// used elsewhere in the program.
fn map_drain_sink_err_to_io_err(_err: std::convert::Infallible) -> std::io::Error {
    panic!("Drain failed somehow!");
}

// For iterators, we shouldn't yield items as quickly as possible, or else the runtime sometimes doesn't give enough
// processing time to other tasks. This creates a stream out of an iterator but also makes sure to yield after every
// element to make sure it can flow through the rest of the system.
fn local_io_stream_from_iter<'a, TIter>(iter: TIter) -> LocalIoStream<'a>
where
    TIter: Iterator<Item = std::io::Result<Bytes>> + 'a,
{
    Box::pin(futures::stream::iter(iter).then(|e| async move {
        tokio::task::yield_now().await;
        e
    }))
}

// Setup an output sink that either goes to stdout or to the void, depending on the user's selection.
fn setup_local_output(args: &NcArgs) -> LocalIoSink {
    let codec = BytesCodec::new();
    match args.output_mode {
        OutputMode::Stdout => Box::pin(FramedWrite::new(tokio::io::stdout(), codec)),
        OutputMode::Null => Box::pin(FramedWrite::new(tokio::io::sink(), codec)),
    }
}

// Tokio's async Stdin implementation doesn't work well for interactive uses. It uses a blocking read, so if we notice
// the socket (not stdin) close, we can't cancel the read on stdin. The user has to hit enter to make the read call
// finish, and then the next read won't be started.
//
// This isn't very nice for interactive uses. Instead, we can set up a separate thread for the blocking stdin read.
// If the remote socket closes then this thread can be exited without having to wait for the async operation to finish.
fn setup_async_stdin_reader_thread(
    chunk_size: u32,
    input_mode: InputMode,
) -> mpsc::UnboundedReceiver<std::io::Result<Bytes>> {
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

                            if let Err(_) =
                                tx_input.unbounded_send(Ok(Bytes::copy_from_slice(&read_buf)))
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
                            tx_input.unbounded_send(Ok(Bytes::copy_from_slice(&read_buf)))
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

// An iterator that generates random u8 values according to the given distribution. Actually wraps that in a
// Result<Bytes> to fit the way it is used to produce a stream of Bytes.
struct RandBytesIter<'a, TDist>
where
    TDist: rand::distributions::Distribution<u8>,
{
    args: &'a NcArgs,

    // TODO: Would rather make this a template type or impl that satisfies Iterator<Item = u8>, but I don't know how to
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
        Self {
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
    type Item = std::io::Result<Bytes>;

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
        Some(Ok(Bytes::copy_from_slice(
            &self.chunk_storage[0..next_size],
        )))
    }
}

fn setup_random_io(args: &NcArgs) -> LocalIoSinkAndStream {
    // Decide the type of data to be produced, depending on user choice. Have to specify the dynamic type here because
    // the match arms have different concrete types (different Distribution implementations).
    let rng_iter: LocalIoStream = match args.rand_config.vals {
        RandValueType::Binary => {
            // Use a standard distribution across all u8 values.
            local_io_stream_from_iter(RandBytesIter::new(args, rand::distributions::Standard))
        }
        RandValueType::Ascii => {
            // Make a distribution that references only ASCII characters. Slice returns a reference to the element in the
            // original array, so map and dereference to return u8 instead of &u8.
            let ascii_dist = rand::distributions::Slice::new(&ASCII_CHARS_STORAGE)
                .unwrap()
                .map(|e| *e);

            local_io_stream_from_iter(RandBytesIter::new(args, ascii_dist))
        }
    };

    (setup_local_output(args), rng_iter)
}

fn setup_local_io(args: &NcArgs) -> LocalIoSinkAndStream {
    // When using TCP, read a byte at a time to send as fast as possible. When using UDP, use the user's requested size
    // to produce datagrams of the correct size.
    let chunk_size = if args.is_udp { args.sendbuf_size } else { 1 };

    match args.input_mode {
        InputMode::Null => (
            setup_local_output(args),
            Box::pin(futures::stream::pending()),
        ),
        InputMode::Stdin | InputMode::StdinNoCharMode => {
            (
                setup_local_output(args),
                // Set up a thread to read from stdin. It will produce only chunks of the required size to send.
                Box::pin(setup_async_stdin_reader_thread(chunk_size, args.input_mode)),
            )
        }

        // Echo mode doesn't have a stream that produces data for sending. That will come directly from the sockets that
        // send data to this machine. It's handled in the routing code.
        InputMode::Echo => (
            setup_local_output(args),
            Box::pin(futures::stream::pending()),
        ),
        InputMode::Random => setup_random_io(args),
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
    targets: &Vec<ConnectionTarget>,
    source_addrs: &SockAddrSet,
    args: &NcArgs,
) -> std::io::Result<()> {
    assert!(!args.af_limit.use_v4 || !args.af_limit.use_v6);
    assert!(!targets.is_empty());

    let mut connections = FuturesUnordered::new();
    let mut router = TcpRouter::new(args);

    // For each user-specified target hostname:port combo, try to connect to all of the addresses it resolved to. When
    // one successful connection is established, move on to the next target. Otherwise we'd end up sending duplicate
    // traffic to the same host.
    for target in targets.iter() {
        for addr in target.addrs.iter() {
            // Skip incompatible candidates from what address family the user specified.
            if args.af_limit.use_v4 && addr.is_ipv6() || args.af_limit.use_v6 && addr.is_ipv4() {
                continue;
            }

            match tcp_connect_to_candidate(addr, source_addrs, args).await {
                Ok(tcp_stream) => {
                    if args.is_zero_io {
                        return Ok(());
                    } else {
                        let peer_addr = tcp_stream.peer_addr().unwrap();

                        // If we were able to connect to a candidate, add them to the router so they can send and receive
                        // traffic.
                        connections.push(handle_tcp_stream(
                            tcp_stream,
                            args,
                            router.add_route(peer_addr),
                        ));

                        // Stop after first successful connection for this target.
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Failed to connect to {}. Error: {}", addr, e);
                }
            }
        }

        // Fail if we couldn't connect to any address for a given target, even if we successfully connected to another
        // target.
        if connections.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("Failed to connect to all candidates for {}", &target.addr_string),
            ));
        }
    }

    loop {
        futures::select! {
            stream_result_opt = connections.next() => {
                match stream_result_opt {
                    Some((result, peer_addr)) => {
                        match result {
                            Ok(_) => eprintln!("Connection to {} finished gracefully.", peer_addr),
                            Err(ref e) => eprintln!("Connection to {} ended with result {}", peer_addr, e),
                        };

                        if connections.len() == 0 {
                            return result;
                        }
                    }
                    None => return Ok(()),
                }
            },
            _ = router.service().fuse() => {
                panic!("Router exited early!");
            }
        };
    }
}

async fn tcp_connect_to_candidate(
    addr: &SocketAddr,
    source_addrs: &SockAddrSet,
    args: &NcArgs,
) -> std::io::Result<tokio::net::TcpStream> {
    eprintln!("Connecting to {}", addr);

    let socket = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()
    } else {
        tokio::net::TcpSocket::new_v6()
    }?;

    configure_socket_options(&socket, addr.is_ipv4(), args)?;

    // Bind the local socket to any local address that matches the address family of the destination.
    let source_addr = source_addrs
        .iter()
        .find(|e| e.is_ipv4() == addr.is_ipv4())
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No matching local address matched destination host's address family",
        ))?;
    socket.bind(*source_addr)?;

    let stream = socket.connect(*addr).await?;

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
) -> std::io::Result<SockAddrSet> {
    assert!(!af_limit.use_v4 || !af_limit.use_v6);

    // If the caller specified a specific address, include that. Otherwise, include all unspecified addresses.
    let mut addrs = SockAddrSet::new();

    if let Some(local_host) = local_host_opt {
        addrs.insert(format!("{}:{}", local_host, local_port)
            .parse()
            .or(Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)))?);
    } else {
        addrs.insert(
            SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::UNSPECIFIED,
                local_port,
            )));

        addrs.insert(
            SocketAddr::V6(std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::UNSPECIFIED,
                local_port,
                0,
                0,
            )));
    }

    // If the caller specified only one address family, filter out any incompatible address families.
    let addrs = addrs
        .drain()
        .filter(|e| !(af_limit.use_v4 && e.is_ipv6() || af_limit.use_v6 && e.is_ipv4()))
        .collect();

    Ok(addrs)
}

async fn do_tcp_listen(listen_addrs: &SockAddrSet, args: &NcArgs) -> std::io::Result<()> {
    let mut listeners = vec![];
    let mut clients = FuturesUnordered::new();
    let mut router = TcpRouter::new(args);

    let max_clients = args.max_clients.unwrap();

    loop {
        // If we ever aren't at maximum clients accepted, start listening on all the specified addresses in order to
        // accept new clients. Only do this if we aren't currently listening.
        if listeners.is_empty() && clients.len() != max_clients {
            // Map the listening addresses to a set of sockets, bind them, and listen on them.
            for listen_addr in listen_addrs.iter() {
                let listening_socket = if listen_addr.is_ipv4() {
                    tokio::net::TcpSocket::new_v4()
                } else {
                    tokio::net::TcpSocket::new_v6()
                }?;

                configure_socket_options(&listening_socket, listen_addr.is_ipv4(), args)?;
                listening_socket.bind(*listen_addr)?;
                let local_addr = listening_socket.local_addr()?;

                eprintln!(
                    "Listening on {}, protocol TCP, family {}",
                    local_addr,
                    if local_addr.is_ipv4() { "IPv4" } else { "IPv6" }
                );

                listeners.push(listening_socket.listen(1)?);
            }
        } else if !listeners.is_empty() && clients.len() == max_clients {
            eprintln!(
                "Not accepting further clients (max {}). Closing listening sockets.",
                max_clients
            );

            // Removing the listening sockets stops the machine from allowing the connection. Remote machines that try
            // to connect to this machine will see a TCP timeout on the connect, which is what we want.
            listeners.clear();
        }

        // Try accepting on all listening sockets. The future will complete when any accept goes through.
        let mut accepts = listeners
            .iter()
            .map(|listener| listener.accept())
            .collect::<FuturesUnordered<_>>();

        futures::select! {
            accept_result = accepts.select_next_some() => {
                match accept_result {
                    Ok((stream, ref peer_addr)) => {
                        eprintln!(
                            "Accepted connection from {}, protocol TCP, family {}",
                            peer_addr,
                            if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" }
                        );

                        // Track the accepted TCP socket here. This future will complete when the socket disconnects.
                        // At the same time, make it known to the router so it can service traffic to and from it.
                        clients.push(handle_tcp_stream(stream, args, router.add_route(*peer_addr)));
                    }
                    Err(e) => {
                        // If there was an error accepting a connection, bail out if the user asked to listen only once.
                        if !args.is_listening_repeatedly {
                            eprintln!("Failed to accept an incoming connection. {}", e);
                            return Err(e);
                        } else {
                            eprintln!("Failed to accept connection: {}", e);
                        }
                    }
                }
            },
            (stream_result, peer_addr) = clients.select_next_some() => {
                match stream_result {
                    Ok(_) => {
                        eprintln!("Connection from {} closed gracefully.", peer_addr);
                    }
                    Err(ref e) => {
                        eprintln!("Connection from {} closed with result: {}", peer_addr, e)
                    }
                };

                // After handling a client, either loop and accept another client or exit, depending on the user's
                // choice.
                if !args.is_listening_repeatedly {
                    return stream_result;
                }
            },
            _ = router.service().fuse() => {
                panic!("Router exited early!");
            },
        };
    }
}

async fn handle_tcp_stream(
    mut tcp_stream: tokio::net::TcpStream,
    args: &NcArgs,
    router_io: RouteSinkAndStream,
) -> (std::io::Result<()>, SocketAddr) {
    let peer_addr = tcp_stream.peer_addr().unwrap();

    // In Zero-IO mode, immediately close the socket. Otherwise, handle it like normal.
    if args.is_zero_io {
        return (Ok(()), peer_addr);
    }

    // The sink is the place where this function can send data coming from the network. The stream is data from the
    // router that should be sent to the network.
    let (mut net_to_router_sink, mut router_to_net_stream) = router_io;

    let (rx_socket, tx_socket) = tcp_stream.split();

    // Set up a sink that sends data out of the TCP socket. This data will come from the router, which gives
    // TargetedBytes, so use fut_remove_target_addr to convert it to just Bytes, which is what the BytesCodec needs.
    let mut router_to_net_sink =
        Box::pin(FramedWrite::new(tx_socket, BytesCodec::new()).with(fut_remove_target_addr));

    // Set up a stream that produces chunks of data from the network. The sink into the router requires SourcedBytes
    // that include the remote peer's address. Also have to convert from BytesMut (which the socket read provides) to
    // just Bytes. If any read error occurs, bubble it up to the disconnection event.
    let mut net_to_router_stream = FramedRead::new(rx_socket, BytesCodec::new()).map(|bm_res| {
        // freeze() to convert BytesMut into Bytes. Add the peer_addr as required for SourcedBytes.
        bm_res.map(|bm| SourcedBytes {
            data: bm.freeze(),
            peer: peer_addr,
        })
    });

    // End when the socket is closed. Return the final error along with the peer address that this socket was connected
    // to, so the program can tell the user which socket closed.
    futures::select! {
        result = router_to_net_sink.send_all(&mut router_to_net_stream).fuse() => {
            (result, peer_addr)
        },
        result = net_to_router_sink.send_all(&mut net_to_router_stream).fuse() => {
            (result, peer_addr)
        },
    }
}

async fn bind_udp_sockets(
    listen_addrs: &SockAddrSet,
    args: &NcArgs,
) -> std::io::Result<Vec<Arc<tokio::net::UdpSocket>>> {
    // Map the listening addresses to a set of sockets, and bind them.
    let mut listening_sockets = vec![];
    for listen_addr in listen_addrs.iter() {
        let socket = tokio::net::UdpSocket::bind(*listen_addr).await?;
        configure_socket_options(&socket, listen_addr.is_ipv4(), args)?;

        listening_sockets.push(Arc::new(socket));

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

    if listening_sockets.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "Could not bind any socket.",
        ));
    }

    Ok(listening_sockets)
}

async fn do_udp_connection(
    targets: &Vec<ConnectionTarget>,
    source_addrs: &SockAddrSet,
    args: &NcArgs,
) -> std::io::Result<()> {
    assert!(!args.af_limit.use_v4 || !args.af_limit.use_v6);

    // Select one address for each candidate to use. Pick the first one of the list, because that might be the preferred
    // choice for DNS load balancing.
    let mut candidates = SockAddrSet::new();
    let mut has_ipv4 = false;
    let mut has_ipv6 = false;
    for target in targets.iter() {
        for addr in target.addrs.iter() {
            // Skip incompatible candidates from what address family the user specified.
            if args.af_limit.use_v4 && addr.is_ipv6() || args.af_limit.use_v6 && addr.is_ipv4() {
                continue;
            }

            // Track whether any IPv4 or IPv6 addresses were encountered, to know which local sockets to bind.
            has_ipv4 |= addr.is_ipv4();
            has_ipv6 |= addr.is_ipv6();

            candidates.insert(*addr);

            // Only use the first address for each target.
            break;
        }
    }

    if candidates.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No suitable remote peers found.",
        ));
    }

    // Filter the source address list to only ones that match the address families of any candidates being used.
    let source_addrs: SockAddrSet = source_addrs
        .iter()
        .filter_map(|e| {
            if (e.is_ipv4() && has_ipv4) || (e.is_ipv6() && has_ipv6) {
                Some(*e)
            } else {
                None
            }
        })
        .collect();

    if source_addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No suitable local address for remote peers.",
        ));
    }

    // Bind to all the source addresses that are needed.
    let sockets = bind_udp_sockets(&source_addrs, args).await?;

    assert!(!sockets.is_empty());

    // If joining multicast, then try to have each source address join each multicast group of a matching address
    // family.
    if args.should_join_multicast_group {
        for socket in sockets.iter() {
            let local_addr = socket.local_addr().unwrap();
            for candidate in candidates.iter().filter(|c| c.is_ipv4() == local_addr.is_ipv4()) {
                join_multicast_group(&**socket, &local_addr, candidate)?;
            }
        }
    }

    handle_udp_sockets(&sockets, Some(&candidates), args).await
}

async fn do_udp_listen(listen_addrs: &SockAddrSet, args: &NcArgs) -> std::io::Result<()> {
    let listeners = bind_udp_sockets(listen_addrs, args).await?;
    handle_udp_sockets(&listeners, None, args).await
}

// Route traffic between local machine and multiple USB peers. The way UDP sockets work, we bind to local sockets and
// then send out of those to remote peer addresses. There is no dedicated socket object (like a TcpStream) that
// represents a remote peer. So we have to establish a conceptual grouping of local UDP socket and remote peer address
// to achieve the same thing.
async fn handle_udp_sockets(
    sockets: &Vec<Arc<tokio::net::UdpSocket>>,
    initial_peers_opt: Option<&SockAddrSet>,
    args: &NcArgs,
) -> std::io::Result<()> {
    fn print_udp_assoc(peer_addr: &SocketAddr) {
        eprintln!(
            "Associating with UDP peer {}, family {}",
            peer_addr,
            if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" }
        );
    }

    let (router_sink, mut inbound_net_traffic_stream) = mpsc::unbounded();
    let router_sink = router_sink.sink_map_err(map_unbounded_sink_err_to_io_err);

    // If the local output (i.e. stdout) fails, we can set this to None to save perf on sending to it further.
    let mut local_io_sink_opt = None;

    // Until the first peer is known, don't start pulling from local input, or else it will get consumed too early.
    let mut local_io_stream: LocalIoStream = Box::pin(futures::stream::pending());

    // Since there is only one local input and output (i.e. stdin/stdout), don't create a new channel to add it to
    // the router. Instead just feed data into the router directly by tagging it as originating from the local
    // input.
    let mut local_io_to_router_sink = Box::pin(
        router_sink
            .clone()
            .with(|b| async { SourcedBytes::ok_from_local(b) }),
    );

    // A collection of all inbound traffic going to the router.
    let mut net_to_router_flows = FuturesUnordered::new();

    // Collect one sink per local socket that can be used to send data out of that socket. Categorize them by IPv4 or
    // IPv6, because the router will try to send data out of each socket to the right destination, but should only use a
    // matching address family or else a send error will occur.
    let mut socket_sinks_ipv4 = vec![];
    let mut socket_sinks_ipv6 = vec![];
    for socket in sockets.iter() {
        // Create a sink and stream for the socket. The sink accepts Bytes and a destination address and turns that into
        // a sendto. The stream produces a Bytes for an incoming datagram and includes the remote source address.
        let framed = tokio_util::udp::UdpFramed::new(socket.clone(), BytesCodec::new());
        let (socket_sink, socket_stream) = framed.split();
        let socket_sinks = if socket.local_addr().unwrap().is_ipv4() {
            &mut socket_sinks_ipv4
        } else {
            &mut socket_sinks_ipv6
        };

        socket_sinks.push(socket_sink);

        // Clone before because this is moved into the async block.
        let mut router_sink = router_sink.clone();

        // Track a future that drives all traffic from the network to the router.
        net_to_router_flows.push(async move {
            let mut socket_stream = socket_stream.filter_map(|bm_res| match bm_res {
                // freeze() to convert BytesMut into Bytes. Add the peer_addr as required for SourcedBytes.
                Ok((bm, peer_addr)) => future::ready(Some(Ok(SourcedBytes {
                    data: bm.freeze(),
                    peer: peer_addr,
                }))),
                Err(e) => {
                    // At the point we receive an error from the socket, there isn't a good way to figure out what
                    // caused it. It might have been the whole network stack going down, or it might have been an ICMP
                    // error response from a previous send to an endpoint that is rejecting the send. In any case, if we
                    // return an error here, it will take down the whole local socket. It would be better to throw away
                    // the error and allow the socket to continue working for other sends.
                    eprintln!("Ignoring failed recv from socket; error={}", e);
                    future::ready(None)
                }
            });

            router_sink.send_all(&mut socket_stream).await
        });
    }

    // Used to figure out when to hook up the local input.
    let mut lifetime_client_count = 0;

    // Track all the known remote addresses that we should route traffic to.
    let mut known_peers = SockAddrSet::new();

    // For outbound UDP scenarios, the caller will pass in the first set of peers to send to (rather than waiting for an
    // inbound peer to make itself known.
    if let Some(initial_peers) = initial_peers_opt {
        for peer in initial_peers.iter() {
            print_udp_assoc(&peer);
            let added = known_peers.insert(*peer);
            assert!(added);
            lifetime_client_count += 1;

            if lifetime_client_count == 1 {
                // Since we have a remote peer hooked up, start processing local IO.
                let (local_io_sink, local_io_stream2) = setup_local_io(args);
                local_io_stream = local_io_stream2;

                assert!(local_io_sink_opt.is_none());
                local_io_sink_opt = Some(local_io_sink);
            }
        }
    }

    // Service multiple different event types in a loop. More notes about this in `TcpRouter::service`.
    loop {
        futures::select! {
            result = net_to_router_flows.select_next_some() => {
                // The streams in this collection never return error and so should never end.
                panic!("net_to_router_flow ended! {:?}", result);
            },
            sb = inbound_net_traffic_stream.select_next_some() => {
                if args.verbose {
                    eprintln!("Router handling traffic: {:?}", sb);
                }

                // On every inbound packet, check if we already know about the remote peer who sent it. If not, start
                // tracking the peer so we can forward traffic to it if needed.
                //
                // If joining a multicast group, don't do this, because it'll end up adding duplicate peers who were
                // going to receive traffic from the multicast group anyway.
                if !args.should_join_multicast_group && sb.peer != LOCAL_IO_PEER_ADDR && known_peers.insert(sb.peer) {
                    lifetime_client_count += 1;

                    print_udp_assoc(&sb.peer);

                    // Don't add the local IO hookup until the first client is added, otherwise the router will pull all
                    // the data out of, say, a redirected input stream, and forward it to nobody, because there are no
                    // other clients.
                    if lifetime_client_count == 1 && local_io_sink_opt.is_none() {
                        let (local_io_sink, local_io_stream2) = setup_local_io(args);
                        local_io_stream = local_io_stream2;
                        local_io_sink_opt = Some(local_io_sink);
                    }
                }

                // Broadcast any incoming data back to whichever other connected sockets and/or local IO it should go
                // to. Track any failed sends so they can be pruned from the list of known routes after.
                let mut broken_routes = SockAddrSet::new();
                for dest_addr in known_peers.iter() {
                    if !should_forward_to(args, &sb.peer, dest_addr) {
                        continue;
                    }

                    if args.verbose {
                        eprintln!("Forwarding to {}", dest_addr);
                    }

                    // You can only send to a destination that uses a matching address family as the local socket.
                    let socket_sinks = if dest_addr.is_ipv4() {
                        &mut socket_sinks_ipv4
                    } else {
                        &mut socket_sinks_ipv6
                    };

                    for socket_sink in socket_sinks.iter_mut() {
                        if let Err(e) = socket_sink.send((sb.data.clone(), *dest_addr)).await {
                            eprintln!("broken route: {:?}", e);
                            broken_routes.insert(*dest_addr);
                        }
                    }
                }

                // Came from a remote endpoint, so also send to local IO.
                if sb.peer != LOCAL_IO_PEER_ADDR {
                    if let Some(ref mut local_io_sink) = local_io_sink_opt {
                        // If we hit an error emitting output, clear out the local output sink so we don't bother
                        // trying to output more.
                        if let Err(e) = local_io_sink.send(sb.data.clone()).await {
                            eprintln!("Local output closed. {}", e);
                            local_io_sink_opt = None;
                        }
                    }
                }

                // If there were any failed sends, clear them out so we don't try to send to them in the future, which
                // would just result in more errors.
                if !broken_routes.is_empty() {
                    for pa in broken_routes.iter() {
                        known_peers.remove(pa);
                    }
                }
            },
            _result = local_io_to_router_sink.send_all(&mut local_io_stream).fuse() => {
                eprintln!("End of outbound data from local machine reached.");
                local_io_stream = Box::pin(futures::stream::pending());
            },
        }
    }
}

#[derive(Args, Clone, Debug)]
#[group(required = false, multiple = false)]
struct AfLimit {
    /// Use IPv4 only
    #[arg(short = '4')]
    use_v4: bool,

    /// Use IPv6 only
    #[arg(short = '6')]
    use_v6: bool,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, clap::ValueEnum)]
enum InputMode {
    /// No input
    #[value(name = "none", alias = "no")]
    Null,

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

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, clap::ValueEnum)]
enum OutputMode {
    /// Output to stdout
    Stdout,

    /// No output
    #[value(name = "none", alias = "no")]
    Null,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, clap::ValueEnum)]
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
r#"connect outbound: nc [options] host:port [host:port ...]
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

    /// Max incoming clients allowed to be connected at the same time. (TCP only).
    #[arg(short = 'm', conflicts_with = "is_udp")]
    max_clients: Option<usize>,

    /// Source address to bind to
    #[arg(short = 's')]
    source_host: Option<String>,

    // Unspecified local port uses port 0, which when bound to assigns from the ephemeral port range.
    /// Port to bind to
    #[arg(short = 'p', default_value_t = 0)]
    source_port: u16,

    /// Broker mode: forward traffic between connected clients. Automatically sets -m 128.
    #[arg(long = "broker")]
    should_broker: bool,

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
    #[arg(short = 'o', value_enum, default_value_t = OutputMode::Stdout)]
    output_mode: OutputMode,

    /// Join multicast group given by hostname (outbound UDP only)
    #[arg(
        long = "mc",
        requires = "is_udp",
        requires = "targets",
    )]
    should_join_multicast_group: bool,

    /// Disable multicast sockets seeing their own traffic
    #[arg(
        long = "mc_no_loop",
        requires = "should_join_multicast_group",
        default_value_t = false
    )]
    should_disable_multicast_loopback: bool,

    #[command(flatten)]
    af_limit: AfLimit,

    #[command(flatten)]
    rand_config: RandConfig,

    /// Emit verbose logging.
    #[arg(short = 'v')]
    verbose: bool,

    /// Host:Port pairs to connect to
    #[arg(value_name= "HOST:PORT", conflicts_with = "is_listening", conflicts_with = "is_listening_repeatedly")]
    targets: Vec<String>,
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

    // If a user is redirecting stdout to a file, then stdin typically starts off at EOF, which makes the local stream
    // end too quickly, so override the input mode to just hang and allow the output to proceed.
    //
    // But if the user is also redirecting stdin from a file, then that should take precedence and allow the input mode
    // to remain so it can read from file.
    match args.input_mode {
        InputMode::Stdin | InputMode::StdinNoCharMode => {
            if std::io::stdin().is_terminal() && !std::io::stdout().is_terminal() {
                eprintln!(
                    "Changing input mode from \"{}\" to \"none\" because stdin is empty.",
                    args.input_mode.to_possible_value().unwrap().get_name()
                );
                args.input_mode = InputMode::Null;
            }
        }
        _ => {}
    }

    // If max_clients wasn't specified explicitly, set its value automatically. If in broker mode, you generally want
    // more than one incoming client at a time, or else why are you in broker mode? Otherwise, safely limit to just one
    // at a time.
    if args.max_clients.is_none() {
        args.max_clients = Some(
            if args.should_broker {
                10
            } else {
                1
            });
    }

    let mut targets: Vec<ConnectionTarget> = vec![];

    if !args.targets.is_empty() {
        eprintln!("Targets:");
        for target in args.targets.iter() {
            let ct = ConnectionTarget::new(target).await.map_err(format_io_err)?;
            eprintln!("{}", ct);
            targets.push(ct);
        }
    }

    // When joining a multicast group, by default you will send traffic to the group but won't receive it unless also
    // bound to the port you're sending to. If the user didn't explicitly choose a local port to bind to, choose the
    // outbound multicast port because it's probably what they actually wanted.
    if args.should_join_multicast_group && args.source_port == 0 {
        if let Some(first_target) = &targets.first() {
            if let Some(first_target_addr) = &first_target.addrs.iter().take(1).next() {
                args.source_port = first_target_addr.port();
            }
        }
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
        if targets.is_empty() {
            usage("Need host:port to connect to!");
        }

        let source_addrs = make_source_addrs()?;

        if args.is_udp {
            do_udp_connection(&targets, &source_addrs, &args).await
        } else {
            do_tcp_connect(&targets, &source_addrs, &args).await
        }
    };

    result.map_err(format_io_err)
}
