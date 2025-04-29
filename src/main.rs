// For AsFd/AsSocket shenanigans.
#![feature(trait_alias)]

extern crate execute;
extern crate regex;

use bytes::{Bytes, BytesMut};
use clap::{Args, CommandFactory, Parser, ValueEnum};
use futures::{
    channel::mpsc, future, future::FusedFuture, stream::FuturesUnordered, FutureExt, SinkExt,
    StreamExt,
};
use rand::{distr::Distribution, Rng};
use regex::Regex;
use std::{
    collections::HashMap,
    collections::HashSet,
    io::{IsTerminal, Write},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

// Some useful general notes about Rust async programming that might help when reading this code here.
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

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct RouteAddr {
    local: SocketAddr,
    peer: SocketAddr,
}

impl std::fmt::Display for RouteAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}<->{}", &self.local, &self.peer)
    }
}

// A `RouteId` is just a smaller identifier to replace the massive `RouteAddr` (64 bytes!) when attaching to a data
// packet flowing from the network to the router. A `Routedb` object is used to maintain a mapping between
// `RouteAddr` <--> `RouteId`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct RouteId(u16);

impl std::fmt::Display for RouteId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

// Due to a bug in the `console` crate (https://github.com/console-rs/console/issues/172), the terminal state can get
// messed up on *nix platforms if the program exits while it's in a blocking read call. So set the default input mode
// to the no-char version, which doesn't have this problem. Users can still opt in to the more interactive mode if they
// want.
const DEFAULT_INPUT_MODE: InputMode = if cfg!(unix) {
    InputMode::StdinNoCharMode
} else {
    InputMode::Stdin
};

// Bytes sourced from the local machine are marked with a route id so they can be treated similarly to
// sockets even when it's not a real socket (and therefore doesn't have a real network address).
const LOCAL_IO_ROUTE_ID: RouteId = RouteId(0);

// From https://learn.microsoft.com/en-us/gaming/playfab/features/multiplayer/servers/using-qos-beacons-to-measure-player-latency-to-azure
// First byte must be 0xFFFF in the datagram to the QoS beacon server. Reply starts with 0x0000 and then echoes the
// request content.
const PFQOS_FIXED_REQUEST_PREFIX: [u8; 2] = [0xffu8, 0xffu8];
const PFQOS_FIXED_RESPONSE_PREFIX: [u8; 2] = [0u8, 0u8];

// A buffer of bytes that carries a route id, which represents both the remote peer that sent it to this machine and the
// local address it was destined for. Used for figuring out which remote machines it should be forwarded to.
#[derive(Debug)]
struct SourcedBytes {
    data: Bytes,
    route_id: RouteId,
}

impl SourcedBytes {
    // Wrap bytes that were produced by the local machine with the special local route address that marks them as
    // originating from the local machine.
    fn create_with_local_source(data: Bytes) -> Self {
        Self {
            data,
            route_id: LOCAL_IO_ROUTE_ID,
        }
    }
}

type SockAddrSet = HashSet<SocketAddr>;
type RouteAddrSet = HashSet<RouteAddr>;
type RouteIdSet = HashSet<RouteId>;

// A future that represents work that drives the local input to completion. It is used with any `InputMode`, regardless
// of how input is obtained (stdin, random generation, etc.).
type LocalInputDriver = Pin<Box<dyn FusedFuture<Output = std::io::Result<()>>>>;

// A sink that accepts byte buffers and sends them to the local IO function (stdout, echo, null, etc.).
type LocalOutputSink = Pin<Box<dyn futures::Sink<Bytes, Error = std::io::Error>>>;

// When setting up local IO, it's common to set up both the way input enters the program and where output from the
// program should go.
type LocalOutputSinkAndInputDriver = (LocalOutputSink, LocalInputDriver);

// A sink that accepts Bytes to be sent to the network. When the router determines that data should be sent to a socket,
// it sends it into this sink, where there is one per remote peer.
type RouterToNetSink<'a> = Pin<Box<dyn futures::Sink<Bytes, Error = std::io::Error> + 'a>>;

// A sink of bytes originating from some remote peer and sent to the router, which uses an mpsc::channel to collect
// data from all sockets. Each socket drives data received on it to the router using this sink, supplying where the data
// came from as well as the local address it arrives at. The router uses the data's origin and local destination to
// decide where the data should be forwarded to. The type looks strange because the associated error has to be mapped to
// `std::io::Error` to fit with what the rest of the program uses.
type RouterSink = futures::sink::SinkMapErr<
    mpsc::UnboundedSender<SourcedBytes>,
    fn(mpsc::SendError) -> std::io::Error,
>;

// A grouping of connection information for a user-specified target, something passed as a command line arg. The
// original argument value is stored as well as all the addresses that the name resolved to.
#[derive(Debug, Clone)]
struct ConnectionTarget {
    addr_string: String,

    // This is stored as a vector instead of a SockAddrSet because we want to preserve the ordering from DNS.
    addrs: Vec<SocketAddr>,
}

impl ConnectionTarget {
    // Do a DNS resolution on an address string and make a ConnectionTarget that contains the original value and the
    // resolved addresses.
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

#[derive(Default, Clone)]
struct Stats {
    send_byte_count: usize,
    send_count: usize,
    recv_byte_count: usize,
    recv_count: usize,
}

struct StatsTracker {
    output_period: Duration,
    stats_since_last_log: Stats,
    last_log_time: Instant,
}

impl StatsTracker {
    fn new(args: &NcArgs) -> Self {
        Self {
            output_period: Duration::from_millis(args.stats_period as u64),
            stats_since_last_log: Stats::default(),
            last_log_time: Instant::now(),
        }
    }

    fn record_send(&mut self, data: &Bytes) {
        self.stats_since_last_log.send_byte_count += data.len();
        self.stats_since_last_log.send_count += 1;
    }

    fn record_recv(&mut self, data: &Bytes) {
        self.stats_since_last_log.recv_byte_count += data.len();
        self.stats_since_last_log.recv_count += 1;
    }

    fn try_consume_stats(&mut self) -> Option<Stats> {
        let now = Instant::now();
        if now.duration_since(self.last_log_time) > self.output_period {
            let ret = self.stats_since_last_log.clone();
            self.stats_since_last_log = Stats::default();
            self.last_log_time = now;
            Some(ret)
        } else {
            None
        }
    }
}

#[macro_use]
extern crate lazy_static;

lazy_static! {
    // Make a static vector with all printable ASCII characters. If we wanted to avoid a static allocation we'd need to
    // implement a sibling of RandBytesIter that contains this vector with it, so that rand::distr::slice::Choose could
    // reference within the object.
    static ref ASCII_CHARS_STORAGE : Vec<u8> = {
        (0u8..=u8::MAX).filter(|i| {
            let c = *i as char;
            c.is_ascii() && !c.is_control()
        }).collect()
    };

    static ref TARGET_MULTIPLIER_REGEX : Regex = Regex::new(r"x(\d+)$").expect("failed to compile regex");

    static ref WILDCARD_HOST_REGEX : Regex = Regex::new(r"^\*(?::(\d+))?$").expect("failed to compile regex");
}

// Core logic used by the router to decide where to forward messages, generally used when not in channel mode.
fn should_forward_to(args: &NcArgs, source: RouteId, dest: RouteId) -> bool {
    // In echo mode, allow sending data back to the source.
    if source == dest
        && args.input_mode != InputMode::Echo
        && args.input_mode != InputMode::PfQosServer
    {
        if args.verbose {
            eprintln!("Skipping forwarding back to source.");
        }

        return false;
    }

    // When in hub mode, any incoming traffic should be sent back to all other peers.
    // When not in hub mode, only send to the local output or back to the source (only possible in
    // echo mode).
    if args.forwarding_mode != ForwardingMode::Hub
        && (dest != source)
        && dest != LOCAL_IO_ROUTE_ID
        && source != LOCAL_IO_ROUTE_ID
    {
        if args.verbose {
            eprintln!("Skipping forwarding to other remote endpoint due to not hub mode.");
        }

        return false;
    }

    true
}

#[derive(Clone)]
struct LatencyStats {
    latest_send_time: Instant,
    latest_send_data: Bytes,
    latest_rtt: Option<Duration>,
}

impl LatencyStats {
    pub fn new() -> Self {
        Self {
            latest_send_time: Instant::now(),
            latest_send_data: Bytes::new(),
            latest_rtt: None,
        }
    }

    pub fn record_send(&mut self, data: Bytes) {
        self.latest_send_time = Instant::now();
        self.latest_send_data = data;

        // If a ping was sent and a caller immediately calls try_get_latest_rtt, it shouldn't get any answer until the
        // response is recorded.
        self.latest_rtt = None;
    }

    pub fn record_recv(&mut self, received_data: &Bytes, args: &NcArgs) {
        // Calculate the RTT as soon as possible in case the checks below take too long and skew the results.
        let potential_rtt = Instant::now().duration_since(self.latest_send_time);

        if received_data.len() < PFQOS_FIXED_RESPONSE_PREFIX.len() {
            if args.verbose {
                eprintln!(
                    "Invalid PfQos response. {} bytes is too short!",
                    received_data.len()
                );
            }
            return;
        }

        let received_prefix = &received_data[..PFQOS_FIXED_RESPONSE_PREFIX.len()];
        if received_prefix != &PFQOS_FIXED_RESPONSE_PREFIX[..] {
            if args.verbose {
                eprintln!("Invalid PfQos prefix! {:?}", received_prefix);
            }
            return;
        }

        let received_payload = &received_data[PFQOS_FIXED_RESPONSE_PREFIX.len()..];
        let latest_sent_payload = &self.latest_send_data[PFQOS_FIXED_RESPONSE_PREFIX.len()..];
        if received_payload != latest_sent_payload {
            if args.verbose {
                eprintln!(
                    "Invalid PfQos payload! received {:?} != latest-sent {:?}",
                    received_payload, latest_sent_payload
                );
            }
            return;
        }

        // All validity checks succeeded, so record the RTT.
        self.latest_rtt = Some(potential_rtt);
    }

    pub fn try_get_latest_rtt(&self) -> Option<Duration> {
        self.latest_rtt
    }
}

struct RouteInfo {
    addr: RouteAddr,
    latency_stats: LatencyStats,
}

impl RouteInfo {
    fn new(addr: RouteAddr) -> Self {
        Self {
            addr,
            latency_stats: LatencyStats::new(),
        }
    }
}

// A database that maps route addresses (64 bytes long!) to a much smaller `RouteId` (only a few bytes). The id ends up
// just being the index in the internal array where the `RouteAddr` is stored, offset by a bit to account for specially
// reserved route ids.
struct RouteDb {
    db: Vec<Option<RouteInfo>>,
}

impl RouteDb {
    // LOCAL_IO_ROUTE_ID is reserved.
    const RESERVED_ROUTE_COUNT: usize = 1;

    pub fn new() -> Self {
        Self { db: vec![] }
    }

    // Mapping functions between a route ID and an index in the array. In particular, a fixed number of slots are
    // reserved for special purposes like indicating local IO.
    fn route_id_from_idx(idx: usize) -> RouteId {
        RouteId((idx + Self::RESERVED_ROUTE_COUNT) as u16)
    }

    fn idx_from_route_id(id: RouteId) -> usize {
        assert!(id.0 as usize >= Self::RESERVED_ROUTE_COUNT);
        id.0 as usize - Self::RESERVED_ROUTE_COUNT
    }

    pub fn add_route(&mut self, new_route: &RouteAddr) -> RouteId {
        // Don't allow adding a route that we already know about. The caller should have used `lookup_id` first if that
        // were a possibility.
        assert!(self.lookup_idx_opt(new_route).is_none());

        // Find any empty slot to reuse.
        let idx = if let Some((idx, _)) = self
            .db
            .iter()
            .enumerate()
            .find(|(_, info_opt)| info_opt.is_none())
        {
            self.db[idx] = Some(RouteInfo::new(new_route.clone()));
            idx

        // If there were no empty slots, add a new one and fill it.
        } else {
            self.db.push(Some(RouteInfo::new(new_route.clone())));
            self.db.len() - 1
        };

        Self::route_id_from_idx(idx)
    }

    pub fn remove_route(&mut self, route_id: RouteId) {
        let idx = Self::idx_from_route_id(route_id);
        assert!(self.db[idx].is_some());
        self.db[idx] = None;
    }

    pub fn lookup_addr(&self, route_id: RouteId) -> &RouteAddr {
        let idx = Self::idx_from_route_id(route_id);
        &self.db[idx].as_ref().unwrap().addr
    }

    pub fn get_latency_stats_mut(&mut self, route_id: RouteId) -> &mut LatencyStats {
        let idx = Self::idx_from_route_id(route_id);
        &mut self.db[idx].as_mut().unwrap().latency_stats
    }

    fn lookup_idx_opt(&self, addr: &RouteAddr) -> Option<usize> {
        self.db
            .iter()
            .enumerate()
            .find(|(_, info_opt)| {
                if let Some(existing_route_info) = info_opt {
                    existing_route_info.addr == *addr
                } else {
                    false
                }
            })
            .map(|(idx, _)| idx)
    }

    pub fn lookup_id(&self, addr: &RouteAddr) -> Option<RouteId> {
        self.lookup_idx_opt(addr).map(Self::route_id_from_idx)
    }
}

// This implements storage and lookup of "channels", which are pairs of remote peers that we should forward traffic
// between but not cross over into other channels. Each channel consists of two routes, one for each remote peer. And
// each route also contains the local address that receives traffic from that peer. This 4-tuple uniquely identifies the
// path a packet takes when it arrives from one endpoint and should be forwarded to the other endpoint. This stores
// `RouteId`s for quicker comparison and can be translated into `RouteAddr`s using a `RouteDb`.
struct ChannelMap {
    channels: HashMap<RouteId, RouteId>,
}

impl ChannelMap {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    fn add_route<'r>(
        &mut self,
        new_route_id: RouteId,
        all_routes: impl Iterator<Item = &'r RouteId>,
        route_db: &RouteDb,
    ) {
        let new_route_addr = route_db.lookup_addr(new_route_id);

        // Nobody should be adding a route that's already part of a channel.
        assert!(!self
            .channels
            .iter()
            .any(|(ra1, ra2)| *ra1 == new_route_id || *ra2 == new_route_id));

        // Check if there is any other route to pair it with.
        for known_route_id in all_routes {
            let known_route_addr = route_db.lookup_addr(*known_route_id);

            // Don't create channels between two ports on the same remote host. Channels are always between different
            // remote hosts. A gap here is that due to NATs, two different remote hosts might appear to come from the
            // same remote IP. A TODO would be to allow a config file or something to decide which remote peers
            // should be permitted to pair up in a channel together.
            if known_route_addr.peer.ip() == new_route_addr.peer.ip() {
                continue;
            }

            // This route is already used in a channel, so it's not available to add to a new one.
            if self.channels.get(known_route_id).is_some() {
                continue;
            }

            eprintln!(
                "Creating channel between {} and {}",
                known_route_addr, new_route_addr
            );

            // Add the channel in both "directions" so it's easy to look up when routing traffic from either remote
            // endpoint.
            let inserted = self.channels.insert(*known_route_id, new_route_id);
            assert!(inserted.is_none());
            let inserted = self.channels.insert(new_route_id, *known_route_id);
            assert!(inserted.is_none());

            // A given route can only be part of at most one channel, so at this point since we added it to a channel,
            // we can stop iterating to look for one.
            break;
        }
    }

    fn get_dest_route(&self, route_id: RouteId) -> Option<RouteId> {
        self.channels.get(&route_id).map(std::clone::Clone::clone)
    }

    fn remove_route(&mut self, route_id: RouteId, route_db: &RouteDb) {
        if self.channels.contains_key(&route_id) {
            eprintln!("Removing failed channel {}", route_db.lookup_addr(route_id));

            // Since we store the channel in both "directions", remove all channels that reference the route to be
            // removed.
            self.channels
                .retain(|ri1, ri2| route_id != *ri1 && route_id != *ri2);
        }
    }
}

// An object that manages a set of sockets connected to known peers. It accepts data from one or more sockets and
// forwards incoming data either just to the local output or to other known peers, depending on the forwarding mode. The
// reason it is specific to TCP is because the sockets that it manages have the destination address embedded in them,
// unlike UDP, where a single socket sends to multiple destinations.
struct TcpRouter<'a> {
    args: &'a NcArgs,

    route_db: RouteDb,

    // A collection of known remote peers, indexed by the route id. Each known peer has a sink that is used for the
    // router to send data to that peer.
    routes: HashMap<RouteId, RouterToNetSink<'a>>,

    // Storage for channels, which are associations of two remote endpoints together for forwarding to each other but
    // not to other channels.
    channels: ChannelMap,

    // A sink where all sockets send data to the router for forwarding. It is an mpsc::channel, so it gets cloned and
    // handed out to each socket.
    router_sink: RouterSink,

    // An internal stream that produces all of the data from all sockets that come into the router.
    inbound_net_traffic_stream: mpsc::UnboundedReceiver<SourcedBytes>,

    // A future that drives the local input to completion.
    local_input_driver: LocalInputDriver,

    // A sink where the router can send data to be printed out.
    local_output_sink: LocalOutputSink,

    // Used to figure out when to hook up the local input.
    lifetime_client_count: u32,

    // When the router is shutting down, give clients a way to check if they should continue interacting with it or not.
    pub is_done: bool,
}

impl<'a> TcpRouter<'a> {
    pub fn new(args: &'a NcArgs) -> TcpRouter<'a> {
        let (router_sink, inbound_net_traffic_stream) = mpsc::unbounded();

        // This funny syntax is required to coerce a function pointer to the fn type required by the field on the struct
        let router_sink = router_sink.sink_map_err(
            map_unbounded_sink_err_to_io_err as fn(mpsc::SendError) -> std::io::Error,
        );

        Self {
            args,
            route_db: RouteDb::new(),
            routes: HashMap::new(),
            channels: ChannelMap::new(),
            router_sink,
            inbound_net_traffic_stream,
            local_input_driver: Box::pin(futures::future::pending()),
            local_output_sink: Box::pin(
                futures::sink::drain().sink_map_err(map_drain_sink_err_to_io_err),
            ),
            lifetime_client_count: 0,
            is_done: false,
        }
    }

    // Callers use this to add a new destination to the router for forwarding. The caller passes in the part of the TCP
    // socket that the router can use to write data to the socket, and it returns a sink where the caller can write data
    // to the router.
    pub fn add_route(
        &mut self,
        tx_socket: tokio::net::tcp::OwnedWriteHalf,
    ) -> (RouteId, RouterSink) {
        let new_route = RouteAddr {
            local: tx_socket.local_addr().unwrap(),
            peer: tx_socket.peer_addr().unwrap(),
        };

        let route_id = self.route_db.add_route(&new_route);

        // Store the sink where the router will send data that should go to this destination.
        let router_to_net_sink: RouterToNetSink =
            Box::pin(FramedWrite::new(tx_socket, BytesCodec::new()));

        self.routes.insert(route_id, router_to_net_sink);

        if self.args.is_using_channels() {
            self.channels
                .add_route(route_id, self.routes.keys(), &self.route_db);
        }

        // Don't add the local IO hookup until the first client is added, otherwise the router will pull all the data
        // out of, say, a redirected input stream, and forward it to nobody, because there are no other clients.
        self.lifetime_client_count += 1;
        if self.lifetime_client_count == 1 {
            (self.local_output_sink, self.local_input_driver) =
                setup_local_io(self.args, self.router_sink.clone());
        }

        // The input end of the router (`router_sink`) can be cloned to allow multiple callers to pass data into the
        // same channel.
        (route_id, self.router_sink.clone())
    }

    pub fn lookup_route_addr(&self, route_id: RouteId) -> &RouteAddr {
        self.route_db.lookup_addr(route_id)
    }

    pub fn remove_route(&mut self, route_id: RouteId) {
        Self::cleanup_route(
            route_id,
            self.args,
            &mut self.route_db,
            &mut self.routes,
            &mut self.channels,
        );

        // Only cleanup the route id when called from outside the router, which means that the calling code will also no
        // longer try to reference the route id.
        self.route_db.remove_route(route_id);
    }

    // This is an associated function because in some codepaths I've already mutably borrowed `self`, so this accepts
    // just the parts of the object that need to be modified.
    fn cleanup_route(
        route_id: RouteId,
        args: &NcArgs,
        route_db: &mut RouteDb,
        routes: &mut HashMap<RouteId, RouterToNetSink>,
        channels: &mut ChannelMap,
    ) {
        // It's possible this route has already been cleaned up or partially cleaned up because the router noticed when
        // forwarding to it that it was closed, or that it was closed as the other end of a channel. Still go through
        // and clean up any lingering state anyway.
        let _removed = routes.remove(&route_id);

        // In channels mode, if a socket at one end of a channel is disconnected, the socket at the other end should
        // also be disconnected, because most programs don't expect upon reconnection to encounter the same socket state
        // as before. So "forward" the disconnection onwards to the other end of the channel. For user who don't want
        // this behavior, the LingerChannels mode will work.
        if args.forwarding_mode == ForwardingMode::Channels {
            // Make sure to grab this before removing the channel below, which would delete this information from the
            // channel map.
            if let Some(channel_dest) = channels.get_dest_route(route_id) {
                // We should only be cleaning up the other end of the channel when we remove the main route. Either both
                // sides of the channel are in the channel map or neither are.
                assert!(_removed.is_some());

                eprintln!(
                    "Also disconnecting other end of channel: {}",
                    route_db.lookup_addr(channel_dest)
                );

                // This doesn't do a full `cleanup_route` because the code outside the router still needs to notice the
                // disconnection, print out info for the user, and cleanup any of its state before calling back in to
                // do the rest of the cleanup.
                let removed = routes.remove(&channel_dest);
                assert!(removed.is_some());
            }
        }

        channels.remove_route(route_id, route_db);

        // TODO: consider at this point looping through all the routes and deciding if we need to pair up new ones.
    }

    // Start asynchronously processing data from all managed sockets.
    pub async fn service(&mut self, args: &NcArgs) -> std::io::Result<()> {
        // If the local output (i.e. stdout) fails, we can set this to None to save perf on sending to it further.
        let mut local_output_sink_opt = Some(&mut self.local_output_sink);

        let mut stats_tracker = StatsTracker::new(&self.args);

        let receive_delay = args.get_receive_delay();

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
                    if receive_delay != Duration::ZERO {
                        tokio::time::sleep(receive_delay).await;
                    }

                    if self.args.verbose {
                        eprintln!("Router handling traffic: {:?}", sb);
                    }

                    // If local IO sent an empty message, that means it was closed.
                    if sb.data.is_empty() && sb.route_id == LOCAL_IO_ROUTE_ID {
                        // The user asked to exit the program after the input stream closed. Clear all the routes, which
                        // will cause graceful disconnections on all of them. Mark the router as done so the caller can
                        // know not to do further work.
                        if self.args.should_exit_after_input_closed {
                            self.is_done = true;

                            // Before disconnecting all the sockets, give a small amount of time to allow in-flight
                            // traffic to be sent.
                            //
                            // TODO: Ideally we'd have some way to figuring this out by tracking when bytes have been
                            // sent to the network rather than a random delay, but I'm not sure how.
                            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                            self.routes.clear();
                            continue;
                        }
                    }

                    let mut broken_routes = RouteIdSet::new();
                    let mut did_send = false;
                    if self.args.is_using_channels() && sb.route_id != LOCAL_IO_ROUTE_ID {
                        // Look up whether this incoming route has a corresponding destination route in a channel.
                        if let Some(channel_dest) = self.channels.get_dest_route(sb.route_id) {
                            if self.args.verbose {
                                eprintln!("Forwarding on channel {} -> {}", self.route_db.lookup_addr(sb.route_id), channel_dest);
                            }

                            // Look up the sink to send to for this route. There always is one, so `unwrap` is OK.
                            let dest_sink = self.routes.get_mut(&channel_dest).unwrap();

                            if let Err(e) = dest_sink.send(sb.data.clone()).await {
                                eprintln!("Error forwarding on channel to {}. {}", self.route_db.lookup_addr(channel_dest), e);
                                broken_routes.insert(channel_dest);
                            } else {
                                stats_tracker.record_send(&sb.data);
                            }

                            did_send = true;
                        } else {
                            if self.args.verbose {
                                eprintln!("Dropping message from {}. No channel found.", self.route_db.lookup_addr(sb.route_id));
                            }
                        }
                    }

                    // If no send happened from channels earlier, try again with regular routing. We shouldn't be in
                    // both hub mode and channel mode, so mostly this covers making sure local input can be sent out,
                    // since it isn't associated with any channel.
                    if !did_send {
                        // Broadcast any incoming data back to whichever other connected sockets and/or local IO it
                        // should go to. Track any failed sends so they can be pruned from the list of known routes
                        // after.
                        for (dest_route, dest_sink) in self.routes.iter_mut() {
                            if !should_forward_to(self.args, sb.route_id, *dest_route) {
                                continue;
                            }

                            if self.args.verbose {
                                eprintln!("Forwarding {}", self.route_db.lookup_addr(*dest_route));
                            }

                            // It could be possible to omit the peer address for the TcpRouter, because the peer address
                            // is implicit with the socket, but I'm going to keep it this way for symmetry for now,
                            // until it becomes a perf problem.
                            if let Err(e) = dest_sink.send(sb.data.clone()).await {
                                eprintln!("Error forwarding to {}. {}", self.route_db.lookup_addr(*dest_route), e);
                                broken_routes.insert(*dest_route);
                            } else {
                                stats_tracker.record_send(&sb.data);
                            }
                        }
                    }

                    // Came from a remote endpoint, so also send to local output if it hasn't failed yet.
                    if sb.route_id != LOCAL_IO_ROUTE_ID {
                        stats_tracker.record_recv(&sb.data);
                        if let Some(ref mut local_output_sink) = local_output_sink_opt {
                            // If we hit an error emitting output, clear out the local output sink so we don't bother
                            // trying to output more.
                            if let Err(e) = local_output_sink.send(sb.data.clone()).await {
                                eprintln!("Local output closed. {}", e);
                                local_output_sink_opt = None;
                            }
                        }
                    }

                    // If there were any failed sends, clear them out so we don't try to send to them in the future,
                    // which would just result in more errors.
                    if !broken_routes.is_empty() {
                        for route in broken_routes.iter() {
                            Self::cleanup_route(*route, self.args, &mut self.route_db, &mut self.routes, &mut self.channels);
                        }
                    }

                    // Output throughput stats periodically if requested by the user.
                    if self.args.should_track_stats {
                        if let Some(stats) = stats_tracker.try_consume_stats() {
                            eprintln!("recv {:>12} B/s ({:>7} recvs/s), send {:>12} B/s ({:>7} sends/s)",
                                stats.recv_byte_count,
                                stats.recv_count,
                                stats.send_byte_count,
                                stats.send_count);
                        }
                    }
                },

                // Drive local input to the router until it completes. `local_input_driver` was previously already
                // passed the `router_sink` so it knows to send it to the router.
                _result = &mut self.local_input_driver => {
                    eprintln!("End of outbound data from local machine reached.");

                    // Send an empty message as a special signal that the local stream has finished.
                    self.router_sink.send(SourcedBytes::create_with_local_source(Bytes::new())).await.expect("Router sink should not be closed early!");
                    self.local_input_driver = Box::pin(futures::future::pending());
                },
            }
        }
    }
}

// Convenience method for formatting an io::Error to String.
fn format_io_err(err: std::io::Error) -> String {
    format!("{}: {}", err.kind(), err)
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
fn local_input_driver_from_iter<TIter>(
    iter: TIter,
    delay: Duration,
    mut router_sink: RouterSink,
) -> LocalInputDriver
where
    TIter: Iterator<Item = std::io::Result<Bytes>> + 'static,
{
    // Transfer ownership of the iterator and router sink into the async block.
    Box::pin(
        async move {
            let mut stream = Box::pin(futures::stream::iter(iter).then(|e| async {
                if delay == Duration::ZERO {
                    tokio::task::yield_now().await;
                } else {
                    tokio::time::sleep(delay).await;
                }
                e.map(SourcedBytes::create_with_local_source)
            }));

            router_sink.send_all(&mut stream).await
        }
        .fuse(),
    )
}

// Setup an output sink that either goes to stdout or to the void, depending on the user's selection.
fn setup_local_output(args: &NcArgs) -> LocalOutputSink {
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
//
// Turns out this strategy works well for both stdin itself and other synchronous Read implementors, such as a child
// process stdout.
fn setup_async_reader_thread<TInput>(
    mut input_reader: TInput,
    mut router_sink: RouterSink,
    args: &NcArgs,
) -> LocalInputDriver
where
    TInput: std::io::Read + std::marker::Send + 'static,
{
    // Helper function to send the current buffer and reset it so it can be used for the next chunk.
    async fn send_and_reset_buffer(
        read_buf: &mut BytesMut,
        router_sink: &mut RouterSink,
        chunk_size: usize,
        alloc_size: usize,
        delay: Duration,
    ) -> std::io::Result<()> {
        if delay != Duration::ZERO {
            tokio::time::sleep(delay).await;
        }

        router_sink
            .send(SourcedBytes::create_with_local_source(
                read_buf.split().freeze(),
            ))
            .await?;

        // After `split`, the original buffer is empty, so it needs to be reallocated and have its length set again so
        // it can be used for the next chunk.
        //
        // SAFETY: We'll only access the portion of the buffer that was written to.
        assert!(read_buf.capacity() % chunk_size == 0);
        if read_buf.capacity() == 0 {
            // Allocate a fresh buffer instead of calling `reserve`. While it's true that `reserve` can do fancy things
            // like reuse the allocated buffer if there are no further handles to it, in practice parallelism prevents
            // the optimal condition from occurring, and instead memory usage just balloons as this keeps adding to the
            // inner buffer.
            *read_buf = BytesMut::with_capacity(alloc_size);
        }

        assert!(read_buf.capacity() >= chunk_size);
        unsafe { read_buf.set_len(chunk_size) };

        Ok(())
    }

    // If the user is interactively using the program, by default use "character mode", which inputs a character on
    // every keystroke rather than a full line when hitting enter. However, the user can request to use normal stdin
    // mode, and if this is reading from a child program execution, that definitely can't use character mode.
    let is_interactive_mode = args.is_interactive_input_mode();
    let chunk_size = args.send_size as usize;
    let delay = args.get_send_delay();

    Box::pin(
        tokio::task::spawn(async move {
            // Create storage for the input from stdin. It needs to be big enough to store the user's desired chunk size
            // It will accumulate data until it's filled an entire chunk, at which point it will be sent and repurposed
            // for the next chunk.
            //
            // I attempted some experiments with reserving more space up front to reduce the frequency of allocations
            // but was unable to measure a difference, so we'll stick with less memory consumption.
            //
            // SAFETY: Only the portion that's written to is accessed. The uninitialized portion is not accessed.
            let alloc_size = chunk_size * 1;
            let mut read_buf = BytesMut::with_capacity(alloc_size);
            unsafe { read_buf.set_len(chunk_size) };

            // The available buffer (pointer to next byte to write and remaining available space in the chunk).
            let mut available_buf = &mut read_buf[..];

            let result = if is_interactive_mode {
                // The console crate has a function called stdout that gives you, uh, a Term object that also services
                // input. OK dude.
                let mut stdio = console::Term::stdout();

                // A buffer specifically for encoding a single `char` read by the console crate.
                let mut char_buf = [0u8; std::mem::size_of::<char>()];
                let char_buf_len = char_buf.len();

                'outer: loop {
                    let read_result = stdio.read_char();
                    if let Ok(ch) = read_result {
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

                            // Copy as much of the char as possible into the chunk. Note that for UTF-8 characters sent
                            // in UDP datagrams, if we fail to copy the whole character into a given datagram, the
                            // resultant traffic might be... strange. Imagine having a UTF-8 character split across two
                            // datagrams. Not great, but also not lossy, so who is to say what is correct? It's not
                            // possible to fully fill every UDP datagram of arbitrary size with varying size UTF-8
                            // characters and not sometimes slice them.
                            available_buf[..num_bytes_copyable]
                                .copy_from_slice(&char_bytes_remaining[..num_bytes_copyable]);

                            // Update the available buffer to the remaining space in the chunk after copying in this
                            // char.
                            available_buf = &mut available_buf[num_bytes_copyable..];

                            // Advance the remaining slice of the char to the uncopied portion.
                            char_bytes_remaining = &mut char_bytes_remaining[num_bytes_copyable..];

                            // There's no more available buffer in this chunk, meaning we've accumulated a full chunk,
                            // so send it now.
                            if available_buf.len() == 0 {
                                let send_result = send_and_reset_buffer(
                                    &mut read_buf,
                                    &mut router_sink,
                                    chunk_size,
                                    alloc_size,
                                    delay,
                                )
                                .await;
                                if send_result.is_err() {
                                    break 'outer send_result;
                                }

                                // The chunk was sent. Reset the available buffer to allow storing the next chunk.
                                available_buf = &mut read_buf[..];
                            }
                        }
                    } else {
                        break read_result.map(|_| ());
                    }
                }
            } else {
                loop {
                    if let Ok(num_bytes_read) = input_reader.read(available_buf) {
                        if num_bytes_read == 0 {
                            // EOF. Disconnect from the channel too.
                            if let Err(e) = router_sink.close().await {
                                eprintln!("Failed to close router sink. Error {}", e);
                            }

                            break Ok(());
                        }

                        assert!(num_bytes_read <= available_buf.len());

                        // We've accumulated a full chunk, so send it now.
                        if num_bytes_read == available_buf.len() {
                            let send_result = send_and_reset_buffer(
                                &mut read_buf,
                                &mut router_sink,
                                chunk_size,
                                alloc_size,
                                delay,
                            )
                            .await;
                            if send_result.is_err() {
                                break send_result;
                            }

                            // The chunk was sent. Reset the buffer to allow storing the next chunk.
                            available_buf = &mut read_buf[..];
                        } else {
                            // Read buffer isn't full yet. Set the available buffer to the rest of the buffer just past
                            // the portion that was written to.
                            available_buf = &mut available_buf[num_bytes_read..];
                        }
                    }
                }
            };

            result
        })
        .map(std::result::Result::unwrap)
        .fuse(),
    )
}

// An iterator that generates random u8 values according to the given distribution. Actually wraps that in a
// Result<Bytes> to fit the way it is used to produce a stream of Bytes.
struct RandBytesIter {
    config: RandConfig,
    rand_iter: Box<dyn Iterator<Item = u8>>,
    rng: rand::rngs::ThreadRng,
    fixed_prefix: Bytes,
}

// Despite the name, this can also be used to send a fixed, unchanging payload repeatedly, if you pass `config.size_max`
// as zero. `new_fixed` is provided as a convenience method to create such an iterator.
impl RandBytesIter {
    fn new(config: &RandConfig, fixed_prefix: Bytes) -> Self {
        // Decide the type of data to be produced, depending on user choice. Have to specify the dynamic type here
        // because the match arms have different concrete types (different Distribution implementations).
        let rand_iter: Box<dyn Iterator<Item = u8>> = match config.vals {
            RandValueType::Binary => {
                // Use a standard distribution across all u8 values.
                Box::new(rand::rng().sample_iter(rand::distr::StandardUniform))
            }
            RandValueType::Ascii => {
                // Make a distribution that references only ASCII characters. Choose returns a reference to the element
                // in the original array, so map and dereference to return u8 instead of &u8.
                let ascii_dist = rand::distr::slice::Choose::new(&ASCII_CHARS_STORAGE)
                    .unwrap()
                    .map(std::clone::Clone::clone);

                Box::new(rand::rng().sample_iter(ascii_dist))
            }
        };

        Self {
            config: config.clone(),
            rand_iter,
            rng: rand::rng(),
            fixed_prefix,
        }
    }

    // Produces the same fixed prefix repeatedly with no random component.
    fn new_fixed(fixed_prefix: Bytes) -> Self {
        Self::new(
            &RandConfig {
                size_min: 0,
                size_max: 0,
                vals: RandValueType::Binary,
            },
            fixed_prefix,
        )
    }
}

impl Iterator for RandBytesIter {
    type Item = std::io::Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        // If there is no random portion, then just copy out the fixed portion.
        if self.config.size_max == 0 {
            Some(Ok(self.fixed_prefix.clone()))
        } else {
            // Figure out the next random size of the chunk.
            let next_size = self.fixed_prefix.len()
                + self
                    .rng
                    .random_range(self.config.size_min..=self.config.size_max);

            let mut chunk_storage = BytesMut::with_capacity(next_size);

            // I was able to measure it much faster to set the length once up front and index into the buffer to write it
            // compared to calling put_u8 repeatedly.
            //
            // SAFETY: `set_len` leaves an uninitialized buffer. Here we're definitely filling in the entire space with
            // random data.
            unsafe { chunk_storage.set_len(next_size) };

            // Copy the fixed portion in first, if present.
            if !self.fixed_prefix.is_empty() {
                chunk_storage[0..self.fixed_prefix.len()].copy_from_slice(&self.fixed_prefix[..]);
            }

            // Fill the rest with random data.
            for x in &mut chunk_storage[self.fixed_prefix.len()..next_size] {
                *x = self.rand_iter.next().unwrap();
            }

            Some(Ok(chunk_storage.freeze()))
        }
    }
}

fn setup_local_io(args: &NcArgs, router_sink: RouterSink) -> LocalOutputSinkAndInputDriver {
    if let Some(command) = args.exec_command_opt.as_ref() {
        // Pipe stdin and stdout from the child process here so it can be wired up to the network.
        let mut prog = execute::shell(command)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to execute command");

        return (
            Box::pin(FramedWrite::new(
                tokio::process::ChildStdin::from_std(prog.stdin.take().unwrap()).unwrap(),
                BytesCodec::new(),
            )),
            Box::pin(setup_async_reader_thread(
                prog.stdout.take().unwrap(),
                router_sink,
                args,
            )),
        );
    }

    match args.input_mode {
        // Echo mode doesn't have a stream that produces data for sending. That will come directly from the sockets that
        // send data to this machine. It's handled in the routing code. PfQosServer is basically echo mode with a little
        // extra logic before sending.
        InputMode::Null | InputMode::Echo | InputMode::PfQosServer => (
            setup_local_output(args),
            Box::pin(futures::future::pending()),
        ),
        InputMode::Stdin | InputMode::StdinNoCharMode => (
            setup_local_output(args),
            // Set up a thread to read from stdin. It will produce only chunks of the required size to send.
            Box::pin(setup_async_reader_thread(
                std::io::stdin(),
                router_sink,
                args,
            )),
        ),
        InputMode::Random => (
            setup_local_output(args),
            local_input_driver_from_iter(
                RandBytesIter::new(&args.rand_config, Bytes::new()),
                args.get_send_delay(),
                router_sink,
            ),
        ),
        InputMode::Fixed => (setup_local_output(args), {
            // Create a random buffer of the size requested by send_size and containing data that matches the
            // user's requested random values configuration.
            let conf = RandConfig {
                size_min: args.send_size as usize,
                size_max: args.send_size as usize,
                vals: args.rand_config.vals,
            };

            let fixed_buf = RandBytesIter::new(&conf, Bytes::new())
                .next()
                .unwrap()
                .unwrap();
            assert!(fixed_buf.len() == args.send_size as usize);

            // This will just keep sending that same fixed buffer to the router forever.
            local_input_driver_from_iter(
                RandBytesIter::new_fixed(fixed_buf),
                args.get_send_delay(),
                router_sink,
            )
        }),
        InputMode::PfQosClient => (setup_local_output(args), {
            // The PlayFab QoS protocol requires a fixed prefix to each datagram's payload. In the response, it will
            // invert the prefix bytes and echo the payload.
            //
            // Generate random payloads with the same fixed prefix.
            let fixed_buf = Bytes::from_static(&PFQOS_FIXED_REQUEST_PREFIX);
            local_input_driver_from_iter(
                RandBytesIter::new(&args.rand_config, fixed_buf),
                Duration::from_millis(args.stats_period as u64),
                router_sink,
            )
        }),
    }
}

// Any socket, TCP or UDP, implements one of these traits for converting it to the OS's native socket handle type. To
// use this with `socket2::SockRef` below, it needs to accept one of these traits, depending on which platform we're on.
#[cfg(windows)]
pub trait AsSocketHandleType = std::os::windows::io::AsSocket;

#[cfg(unix)]
pub trait AsSocketHandleType = std::os::fd::AsFd;

fn configure_socket_options<S>(socket: &S, is_ipv4: bool, args: &NcArgs) -> std::io::Result<()>
where
    S: AsSocketHandleType,
{
    let s2 = socket2::SockRef::from(socket);
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

    if let Some(sendbuf_size) = args.sendbuf_size_opt {
        s2.set_send_buffer_size(sendbuf_size as usize)?;
    }

    if let Some(recvbuf_size) = args.recvbuf_size_opt {
        s2.set_recv_buffer_size(recvbuf_size as usize)?;
    }

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
    S: AsSocketHandleType,
{
    let s2 = socket2::SockRef::from(socket);
    let interface_index = get_interface_index_from_local_addr(local_addr)?;

    match multi_addr {
        SocketAddr::V4(addr) => s2.join_multicast_v4_n(
            addr.ip(),
            &socket2::InterfaceIndexOrAddress::Index(interface_index),
        ),
        SocketAddr::V6(addr) => s2.join_multicast_v6(addr.ip(), interface_index),
    }
}

async fn tcp_connect_to_candidate(
    addr: &SocketAddr,
    source_addrs: &SockAddrSet,
    args: &NcArgs,
) -> std::io::Result<tokio::net::TcpStream> {
    // Bind the local socket to any local address that matches the address family of the destination.
    let source_addr = source_addrs
        .iter()
        .find(|e| e.is_ipv4() == addr.is_ipv4())
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No matching local address matched destination host's address family",
        ))?;

    eprintln!("Connecting from {} to {}", source_addr, addr);

    let socket = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()
    } else {
        tokio::net::TcpSocket::new_v6()
    }?;

    configure_socket_options(&socket, addr.is_ipv4(), args)?;

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

async fn get_local_addrs(
    local_addr_strings: impl Iterator<Item = &str>,
    include_unspec_as_default: bool,
    args: &NcArgs,
) -> std::io::Result<SockAddrSet> {
    assert!(!args.af_limit.use_v4 || !args.af_limit.use_v6);

    let mut local_addrs = SockAddrSet::new();

    let mut did_lookup = false;
    for addr_string in local_addr_strings {
        did_lookup = true;
        if args.verbose {
            eprintln!("Looking up {}", addr_string);
        }

        // If this matches, then the user has passed * or *:NNNN and wants to use the wildcard address either with port
        // 0 implicitly or with a specified port.
        if let Some(captures) = WILDCARD_HOST_REGEX.captures_iter(addr_string).next() {
            // Get the first capture if it's present, which is the port number.
            let port_num = if let Some(port_match) = captures.get(1) {
                // Unwrap is OK here because the regex validated that this is a number only.
                port_match.as_str().parse::<u16>().unwrap()
            } else {
                0
            };

            local_addrs.insert(SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::UNSPECIFIED,
                port_num,
            )));

            local_addrs.insert(SocketAddr::V6(std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::UNSPECIFIED,
                port_num,
                0,
                0,
            )));
        } else {
            let addrs: SockAddrSet = tokio::net::lookup_host(&addr_string).await?.collect();
            for addr in addrs.iter() {
                if args.verbose {
                    eprintln!("Resolved to {}", addr);
                }

                local_addrs.insert(*addr);
            }
        }
    }

    // The caller may optionally choose to default to including the wildcard local addresses if they didn't pass any
    // specific one.
    if !did_lookup && include_unspec_as_default {
        local_addrs.insert(SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            0,
        )));

        local_addrs.insert(SocketAddr::V6(std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::UNSPECIFIED,
            0,
            0,
            0,
        )));
    }

    // If the caller specified only one address family, filter out any incompatible address families.
    let local_addrs = local_addrs
        .drain()
        .filter(|e| !(args.af_limit.use_v4 && e.is_ipv6() || args.af_limit.use_v6 && e.is_ipv4()))
        .collect();

    Ok(local_addrs)
}

async fn do_tcp(
    listen_addrs: &SockAddrSet,
    outbound_source_addrs: &SockAddrSet,
    targets: &[ConnectionTarget],
    args: &NcArgs,
) -> std::io::Result<()> {
    let mut outbound_connections = FuturesUnordered::new();
    let mut inbound_connections = FuturesUnordered::new();
    let mut listeners = vec![];
    let mut router = TcpRouter::new(args);

    let max_inbound_connections = args.max_inbound_connections.unwrap();

    // For each user-specified target hostname:port combo, try to connect to all of the addresses it resolved to. When
    // one successful connection is established, move on to the next target. Otherwise we'd end up sending duplicate
    // traffic to the same host.
    for target in targets.iter() {
        let mut succeeded_any_connection = false;
        for addr in target.addrs.iter() {
            // Skip incompatible candidates from what address family the user specified.
            if args.af_limit.use_v4 && addr.is_ipv6() || args.af_limit.use_v6 && addr.is_ipv4() {
                continue;
            }

            match tcp_connect_to_candidate(addr, outbound_source_addrs, args).await {
                Ok(tcp_stream) => {
                    // If we were able to connect to a candidate, add them to the router so they can send and receive
                    // traffic.
                    let (rx_socket, tx_socket) = tcp_stream.into_split();
                    let route_info = router.add_route(tx_socket);
                    outbound_connections.push(handle_tcp_stream(rx_socket, args, route_info));

                    // Stop after first successful connection for this target.
                    succeeded_any_connection = true;
                    break;
                }
                Err(e) => {
                    eprintln!("Failed to connect to {}. Error: {}", addr, e);
                }
            }
        }

        // Fail if we couldn't connect to any address for a given target, even if we successfully connected to another
        // target.
        if !succeeded_any_connection {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("Failed to connect to {}", &target.addr_string),
            ));
        }
    }

    let mut last_result = Ok(());
    loop {
        // If we ever aren't at maximum clients accepted, start listening on all the specified addresses in order to
        // accept new clients. Only do this if we aren't currently listening.
        if listeners.is_empty() && inbound_connections.len() != max_inbound_connections {
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
        } else if !listeners.is_empty() && inbound_connections.len() == max_inbound_connections {
            eprintln!(
                "Not accepting further clients (max {}). Closing listening sockets.",
                max_inbound_connections
            );

            // Removing the listening sockets stops the machine from allowing the connection. Remote machines that try
            // to connect to this machine will see a TCP timeout on the connect, which is what we want.
            listeners.clear();
        }

        // Try accepting on all listening sockets. The future will complete when any accept goes through.
        let mut accepts = listeners
            .iter()
            .map(tokio::net::TcpListener::accept)
            .collect::<FuturesUnordered<_>>();

        futures::select! {
            accept_result = accepts.select_next_some() => {
                match accept_result {
                    Ok((tcp_stream, ref peer_addr)) => {
                        eprintln!(
                            "Accepted connection from {}, protocol TCP, family {}",
                            peer_addr,
                            if peer_addr.is_ipv4() { "IPv4" } else { "IPv6" }
                        );

                        // Track the accepted TCP socket here. This future will complete when the socket disconnects.
                        // At the same time, make it known to the router so it can service traffic to and from it.
                        let (rx_socket, tx_socket) = tcp_stream.into_split();
                        let route_info = router.add_route(tx_socket);
                        inbound_connections.push(handle_tcp_stream(rx_socket, args, route_info));
                    }
                    Err(e) => {
                        // If there was an error accepting a connection, bail out if the user asked to listen only once.
                        if args.listen_many.is_empty() {
                            eprintln!("Failed to accept an incoming connection. {}", e);
                            return Err(e);
                        } else {
                            eprintln!("Failed to accept connection: {}", e);
                        }
                    }
                }
            },
            (stream_result, route_id) = inbound_connections.select_next_some() => {
                let route_addr = router.lookup_route_addr(route_id).clone();
                match stream_result {
                    Ok(_) => {
                        eprintln!("Connection {} closed gracefully.", route_addr);
                    }
                    Err(ref e) => {
                        eprintln!("Connection {} closed with result: {}", route_addr, e)
                    }
                };

                // After handling a client, either loop and accept another client or exit, depending on the user's
                // choice. There are two reasons to exit: the user asked to listen only one time, or the router has shut
                // down.
                if args.listen_many.is_empty() || router.is_done {
                    return stream_result;
                }

                // Notify the router that a connection failed so it can clean it up.
                router.remove_route(route_id);
            },
            (result, route_id) = outbound_connections.select_next_some() => {
                let route_addr = router.lookup_route_addr(route_id).clone();
                // A TcpStream ended. Print out some status and potentially reconnect to it.
                let should_reconnect =
                    match result {
                        Ok(_) => {
                            eprintln!("Connection {} finished gracefully.", route_addr);
                            args.should_reconnect_on_graceful_close
                        }
                        Err(ref e) => {
                            eprintln!("Connection {} ended with result {}", route_addr, e);
                            args.should_reconnect_on_error
                        }
                    };

                last_result = result;

                // Every time a socket closes, it's possible it was because the router finished, so check to
                // make sure the router is still active before doing things like reconnecting.
                if !router.is_done {
                    // Notify the router that a connection failed so it can clean it up. Possible that this happened
                    // because the router itself closed the route, in which case this will have no effect.
                    router.remove_route(route_id);

                    // When reconnecting, just do another connection and add it to the list of ongoing connections
                    // being tracked.
                    if should_reconnect {
                        match tcp_connect_to_candidate(&route_addr.peer, outbound_source_addrs, args).await {
                            Ok(tcp_stream) => {
                                // If we were able to connect to a candidate, add them to the router so they can
                                // send and receive traffic.
                                let (rx_socket, tx_socket) = tcp_stream.into_split();
                                let route_info = router.add_route(tx_socket);
                                outbound_connections.push(handle_tcp_stream(
                                    rx_socket,
                                    args,
                                    route_info,
                                ));
                            }
                            Err(e) => {
                                eprintln!("Failed to connect to {}. Error: {}", route_addr.peer, e);
                            }
                        }
                    }
                }
            },
            _ = router.service(args).fuse() => {
                panic!("Router exited early!");
            },
        };

        // If there is no work to do on any current connections and we aren't in repeated listening mode (which would
        // cause us to imminently start listening again), then there's nothing left to do and we should exit.
        if args.listen_many.is_empty()
            && inbound_connections.is_empty()
            && outbound_connections.is_empty()
            && listeners.is_empty()
            && accepts.is_empty()
        {
            return last_result;
        }
    }
}

async fn handle_tcp_stream(
    rx_socket: tokio::net::tcp::OwnedReadHalf,
    args: &NcArgs,
    (route_id, mut net_to_router_sink): (RouteId, RouterSink),
) -> (std::io::Result<()>, RouteId) {
    // In Zero-IO mode, immediately close the socket. Otherwise, handle it like normal.
    if args.is_zero_io {
        return (Ok(()), route_id);
    }

    // Set up a stream that produces chunks of data from the network. The sink into the router requires SourcedBytes
    // that include the route id, which can be mapped to the remote peer's address. Also have to convert from BytesMut
    // (which the socket read provides) to just Bytes. If any read error occurs, bubble it up to the disconnection
    // event.
    let mut net_to_router_stream = FramedRead::new(rx_socket, BytesCodec::new()).map(|bm_res| {
        // freeze() to convert BytesMut into Bytes. Add the route id as required for SourcedBytes.
        bm_res.map(|bm| SourcedBytes {
            data: bm.freeze(),
            route_id,
        })
    });

    // End when the socket is closed. Return the final error along with the route id that identifies this socket and the
    // remote peer, so the program can tell the user which socket closed.
    (
        net_to_router_sink
            .send_all(&mut net_to_router_stream)
            .fuse()
            .await,
        route_id,
    )
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

async fn do_udp(
    listen_addrs: &SockAddrSet,
    outbound_source_addrs: &SockAddrSet,
    targets: &[ConnectionTarget],
    args: &NcArgs,
) -> std::io::Result<()> {
    assert!(!args.af_limit.use_v4 || !args.af_limit.use_v6);

    let is_compatible_af = |addr: &&SocketAddr| {
        !(args.af_limit.use_v4 && addr.is_ipv6() || args.af_limit.use_v6 && addr.is_ipv4())
    };

    // Take the first address with a compatible address family to the user's preferences and track which address family
    // is used, so we can find out which local sockets need to be bound.
    let mut has_ipv4 = false;
    let mut has_ipv6 = false;
    for target in targets.iter() {
        if let Some(addr) = target.addrs.iter().filter(is_compatible_af).next() {
            has_ipv4 |= addr.is_ipv4();
            has_ipv6 |= addr.is_ipv6();
        }
    }

    // Filter the source address list to only ones that match the address families of any candidates being used.
    let mut bind_addrs: SockAddrSet = outbound_source_addrs
        .iter()
        .filter_map(|e| {
            if (e.is_ipv4() && has_ipv4) || (e.is_ipv6() && has_ipv6) {
                Some(*e)
            } else {
                None
            }
        })
        .collect();

    // To get the whole list of addresses to bind to, add in all the ones the user specified to listen on.
    bind_addrs.extend(listen_addrs.iter());

    if bind_addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No suitable local address for remote peers.",
        ));
    }

    // Bind to all the source addresses that are needed.
    let sockets = bind_udp_sockets(&bind_addrs, args).await?;

    assert!(!sockets.is_empty());

    let mut candidates = RouteAddrSet::new();
    for target in targets.iter() {
        // Select only one address for each target to use, so we don't send duplicate traffic to two addresses from the
        // same machine. Pick the first one of the list, because that might be the preferred choice for DNS load
        // balancing.
        if let Some(candidate) = target.addrs.iter().filter(is_compatible_af).next() {
            for socket in sockets.iter() {
                let route = RouteAddr {
                    local: socket.local_addr().unwrap(),
                    peer: *candidate,
                };
                if route.local.is_ipv4() != route.peer.is_ipv4() {
                    // Skip because incompatible address family.
                    continue;
                }

                candidates.insert(route);

                // Choose the first socket with a matching address family to use to send to the peer.
                break;
            }
        }
    }

    if listen_addrs.is_empty() && candidates.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No suitable remote peers found.",
        ));
    }

    // If joining multicast, then try to have each source address join each multicast group of a matching address
    // family.
    if args.should_join_multicast_group {
        for socket in sockets.iter() {
            let local_addr = socket.local_addr().unwrap();
            for candidate in candidates
                .iter()
                .filter(|c| c.local.is_ipv4() == local_addr.is_ipv4())
            {
                join_multicast_group(&**socket, &local_addr, &candidate.peer)?;
            }
        }
    }

    handle_udp_sockets(&sockets, &candidates, args).await
}

// Route traffic between local machine and multiple UDP peers. The way UDP sockets work, we bind to local sockets and
// then send out of those to remote peer addresses. There is no dedicated socket object (like a TcpStream) that
// represents a remote peer. So we have to establish a conceptual grouping of local UDP socket and remote peer address
// to achieve the same thing.
async fn handle_udp_sockets(
    sockets: &Vec<Arc<tokio::net::UdpSocket>>,
    initial_routes: &RouteAddrSet,
    args: &NcArgs,
) -> std::io::Result<()> {
    fn print_udp_assoc(route_addr: &RouteAddr) {
        eprintln!(
            "Associating {}, family {}",
            route_addr,
            if route_addr.peer.is_ipv4() {
                "IPv4"
            } else {
                "IPv6"
            }
        );
    }

    // This has to be accessed on each UDP stream's async task, because that's where `RouteAddr`s are turned into
    // `RouteId`s.
    let route_db = Arc::new(std::sync::RwLock::new(RouteDb::new()));

    let (router_sink, mut inbound_net_traffic_stream) = mpsc::unbounded();
    let mut router_sink = router_sink
        .sink_map_err(map_unbounded_sink_err_to_io_err as fn(mpsc::SendError) -> std::io::Error);

    // If the local output (i.e. stdout) fails, we can set this to None to save perf on sending to it further.
    let mut local_output_sink_opt = None;

    // Until the first peer is known, don't start pulling from local input, or else it will get consumed too early.
    let mut local_input_driver: LocalInputDriver = Box::pin(futures::future::pending());

    // A collection of all inbound traffic going to the router.
    let mut net_to_router_flows = FuturesUnordered::new();

    // Collect one sink per local socket that can be used to send data out of that socket.
    let mut socket_sinks = HashMap::new();
    for socket in sockets.iter() {
        // Create a sink and stream for the socket. The sink accepts Bytes and a destination address and turns that into
        // a sendto. The stream produces a Bytes for an incoming datagram and includes a route id which can be
        // translated by the `RouteDb` into a local and remote address.
        let local_addr = socket.local_addr().unwrap();
        let framed = tokio_util::udp::UdpFramed::new(socket.clone(), BytesCodec::new());
        let (socket_sink, socket_stream) = framed.split();

        socket_sinks.insert(local_addr, socket_sink);

        // Clone before because this is moved into the async block.
        let mut router_sink = router_sink.clone();

        // Get a new reference to the `RouteDb` to move into the async task to handle the stream.
        let route_db_for_stream = route_db.clone();

        // Track a future that drives all traffic from this socket to the router.
        net_to_router_flows.push(async move {
            let mut socket_stream = socket_stream.filter_map(|bm_res| match bm_res {
                Ok((bm, peer_addr)) => {
                    let route_addr = RouteAddr {
                        local: local_addr,
                        peer: peer_addr,
                    };

                    // On every UDP receive we have to map the route address to a route id, which is what is passed
                    // along in a `SourcedBytes`. First look up the route to see if it's already known by the db. This
                    // is the common case, so most of the time we incur the cost of only a reader lock. Rarely, a new
                    // route needs to be added, and we'll incur the cost of a writer lock.
                    let route_id_opt = route_db_for_stream.read().unwrap().lookup_id(&route_addr);

                    // Make sure to use unwrap_or_else, or the `add_route` call will be made even if `route_id_opt` is
                    // Some.
                    let route_id = route_id_opt.unwrap_or_else(|| {
                        route_db_for_stream.write().unwrap().add_route(&route_addr)
                    });

                    // freeze() to convert BytesMut into Bytes. Add the route id as required for SourcedBytes.
                    future::ready(Some(Ok(SourcedBytes {
                        data: bm.freeze(),
                        route_id,
                    })))
                }
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

    // All known local<->remote endpoints we know about. If someone sends us traffic to one of our local addresses,
    // track it so we know which local socket to use to send replies.
    let mut known_routes = HashSet::new();

    let mut channels = ChannelMap::new();

    // The user may specify a set of initial peers to be aware of and send traffic to. That is typical of outbound
    // scenarios. For inbound-only scenarios, no initial peers are specified because they aren't known yet.
    for route in initial_routes.iter() {
        let route_id = route_db.write().unwrap().add_route(&route);

        print_udp_assoc(&route);
        let added = known_routes.insert(route_id);
        assert!(added);
        lifetime_client_count += 1;

        if args.is_using_channels() {
            channels.add_route(route_id, known_routes.iter(), &route_db.read().unwrap());
        }

        if lifetime_client_count == 1 {
            // Since we have a remote peer hooked up, start processing local IO.
            let (local_output_sink, local_input_driver2) =
                setup_local_io(args, router_sink.clone());
            local_input_driver = local_input_driver2;

            assert!(local_output_sink_opt.is_none());
            local_output_sink_opt = Some(local_output_sink);
        }
    }

    let mut stats_tracker = StatsTracker::new(args);

    let receive_delay = args.get_receive_delay();

    // Service multiple different event types in a loop. More notes about this in `TcpRouter::service`.
    loop {
        futures::select! {
            result = net_to_router_flows.select_next_some() => {
                // The streams in this collection never return error and so should never end.
                panic!("net_to_router_flow ended! {:?}", result);
            },
            mut sb = inbound_net_traffic_stream.select_next_some() => {
                if receive_delay != Duration::ZERO {
                    tokio::time::sleep(receive_delay).await;
                }

                if args.verbose {
                    eprintln!("Router handling traffic: {:?}", sb);
                }

                if args.input_mode == InputMode::PfQosServer {
                    // If this is a valid PfQos request, switch the request header into the response header for sending
                    // back.
                    if sb.data[..PFQOS_FIXED_REQUEST_PREFIX.len()] == PFQOS_FIXED_REQUEST_PREFIX {
                        let mut response_data = BytesMut::from(&sb.data[..]);
                        response_data[..PFQOS_FIXED_RESPONSE_PREFIX.len()].copy_from_slice(&PFQOS_FIXED_RESPONSE_PREFIX[..]);
                        sb.data = response_data.freeze();
                    }
                }

                if sb.route_id == LOCAL_IO_ROUTE_ID {
                    // If local IO sent an empty message, that means it was closed.
                    if args.should_exit_after_input_closed {
                        // Before quitting and therefore disconnecting all the sockets, give a small amount of time to
                        // allow in-flight traffic to be sent.
                        //
                        // TODO: Ideally we'd have some way to figuring this out by tracking when bytes have been
                        // sent to the network rather than a random delay, but I'm not sure how.
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        return Ok(());
                    }
                }

                // On every inbound packet, check if we already know about the remote peer who sent it. If not, start
                // tracking the peer so we can forward traffic to it if needed.
                //
                // If joining a multicast group, don't do this, because it'll end up adding duplicate peers who were
                // going to receive traffic from the multicast group anyway.
                if !args.should_join_multicast_group && sb.route_id != LOCAL_IO_ROUTE_ID && known_routes.insert(sb.route_id) {
                    lifetime_client_count += 1;

                    print_udp_assoc(route_db.read().unwrap().lookup_addr(sb.route_id));

                    if args.is_using_channels() {
                        channels.add_route(sb.route_id, known_routes.iter(), &route_db.read().unwrap());
                    }

                    // Don't add the local IO hookup until the first client is added, otherwise the router will pull all
                    // the data out of, say, a redirected input stream, and forward it to nobody, because there are no
                    // other clients.
                    if lifetime_client_count == 1 && local_output_sink_opt.is_none() {
                        let (local_output_sink, local_input_driver2) = setup_local_io(args, router_sink.clone());
                        local_input_driver = local_input_driver2;
                        local_output_sink_opt = Some(local_output_sink);
                    }
                }

                let mut broken_routes = RouteIdSet::new();
                let mut did_send = false;

                // If using channel routing, look up if there's a known channel that this incoming packet should be
                // forwarded to.
                if args.is_using_channels() {
                    if let Some(channel_dest) = channels.get_dest_route(sb.route_id) {
                        let channel_dest_route_addr = route_db.read().unwrap().lookup_addr(channel_dest).clone();

                        if args.verbose {
                            eprintln!("Forwarding on channel {} -> {}", route_db.read().unwrap().lookup_addr(sb.route_id), channel_dest_route_addr);
                        }

                        // There should always be a backing sink for any channel route.
                        let dest_sink = socket_sinks.get_mut(&channel_dest_route_addr.local).unwrap();

                        if let Err(e) = dest_sink.send((sb.data.clone(), channel_dest_route_addr.peer)).await {
                            eprintln!("Error forwarding on channel to {}. {}", channel_dest_route_addr.peer, e);
                            broken_routes.insert(channel_dest);
                        } else {
                            if args.input_mode == InputMode::PfQosClient {
                                route_db.write().unwrap().get_latency_stats_mut(channel_dest).record_send(sb.data.clone());
                            }

                            stats_tracker.record_send(&sb.data);
                        }

                        did_send = true;
                    } else {
                        if args.verbose {
                            eprintln!("Dropping message from {}. No channel found.", route_db.read().unwrap().lookup_addr(sb.route_id));
                        }
                    }
                }

                // If no send happened from channels earlier, try again with regular routing. We shouldn't be in both
                // hub mode and channel mode, so mostly this covers making sure local input can be sent out, since
                // it isn't associated with any channel.
                //
                // Broadcast any incoming data back to whichever other connected sockets and/or local IO it should go
                // to. Track any failed sends so they can be pruned from the list of known routes after.
                if !did_send {
                    for dest_route in known_routes.iter() {
                        let dest_route_addr = route_db.read().unwrap().lookup_addr(*dest_route).clone();
                        if !should_forward_to(args, sb.route_id, *dest_route) {
                            continue;
                        }

                        if args.verbose {
                            eprintln!("Forwarding to {}", dest_route_addr);
                        }

                        if let Some(socket_sink) = socket_sinks.get_mut(&dest_route_addr.local) {
                            if let Err(e) = socket_sink.send((sb.data.clone(), dest_route_addr.peer)).await {
                                eprintln!("Error forwarding to {}. {}", dest_route_addr.peer, e);
                                broken_routes.insert(*dest_route);
                            } else {
                                if args.input_mode == InputMode::PfQosClient {
                                    route_db.write().unwrap().get_latency_stats_mut(*dest_route).record_send(sb.data.clone());
                                }

                                stats_tracker.record_send(&sb.data);
                            }
                        } else {
                            eprintln!("No sink found for {}", dest_route_addr.local);
                        }
                    }
                }

                // Came from a remote endpoint, so also send to local IO.
                if sb.route_id != LOCAL_IO_ROUTE_ID {
                    if args.input_mode == InputMode::PfQosClient {
                        let mut route_db_lock = route_db.write().unwrap();
                        let latency_stats = route_db_lock.get_latency_stats_mut(sb.route_id);
                        latency_stats.record_recv(&sb.data, args);
                        if let Some(latest_rtt) = latency_stats.try_get_latest_rtt() {
                            let route_addr = route_db_lock.lookup_addr(sb.route_id).clone();
                            eprintln!("{} Round trip time: {:>5} ms", route_addr, latest_rtt.as_millis());
                        }
                    }

                    stats_tracker.record_recv(&sb.data);

                    if let Some(ref mut local_output_sink) = local_output_sink_opt {
                        // If we hit an error emitting output, clear out the local output sink so we don't bother
                        // trying to output more.
                        if let Err(e) = local_output_sink.send(sb.data.clone()).await {
                            eprintln!("Local output closed. {}", e);
                            local_output_sink_opt = None;
                        }
                    }
                }

                // If there were any failed sends, clear them out so we don't try to send to them in the future, which
                // would just result in more errors.
                if !broken_routes.is_empty() {
                    for route in broken_routes.iter() {
                        known_routes.remove(route);

                        if args.is_using_channels() {
                            if let Some(channel_dest) = channels.get_dest_route(*route) {
                                channels.remove_route(channel_dest, &route_db.read().unwrap());
                            }
                        }

                        channels.remove_route(*route, &route_db.read().unwrap());
                        route_db.write().unwrap().remove_route(*route);
                    }
                }

                // TODO: would love to get rid of the duplication between this and TcpRouter, but it's just slightly
                // different.
                //
                // Output throughput stats periodically if requested by the user.
                if args.should_track_stats {
                    if let Some(stats) = stats_tracker.try_consume_stats() {
                        eprintln!("recv {:>12} B/s ({:>7} p/s), send {:>12} B/s ({:>7} p/s)",
                            stats.recv_byte_count,
                            stats.recv_count,
                            stats.send_byte_count,
                            stats.send_count);
                    }
                }
            },
            _result = &mut local_input_driver => {
                eprintln!("End of outbound data from local machine reached.");
                local_input_driver = Box::pin(futures::future::pending());

                // Send an empty message as a special signal that the local stream has finished.
                router_sink.send(SourcedBytes::create_with_local_source(Bytes::new())).await.expect("Local IO sink should not be closed early!");
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
    #[value(name = "none", alias = "n")]
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

    /// Repeatedly send the same random buffer forever. The size of the buffer is controlled by --sb and the contents are controlled by --rvals. Only useful for perf testing, really
    #[value(name = "fixed", alias = "f")]
    Fixed,

    /// Act as a client for PlayFab Quality of Service beacon servers, measuring round trip latency.
    #[value(name = "pfqoscli")]
    PfQosClient,

    /// Act as PlayFab Quality of Service beacon servers, for clients to send latency measurement pings to.
    #[value(name = "pfqossrv")]
    PfQosServer,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, clap::ValueEnum)]
enum OutputMode {
    /// Output to stdout
    Stdout,

    /// No output
    #[value(name = "none", alias = "n")]
    Null,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, clap::ValueEnum)]
enum ForwardingMode {
    /// No forwarding
    #[value(name = "none")]
    Null,

    /// Hub mode: forward traffic between connected clients. Automatically sets -m 10.
    #[value(name = "hub", alias = "h")]
    Hub,

    /// Channel mode: automatically group pairs of remote addresses from different IP addresses into "channels" and forward traffic between the two endpoints, but not between different channels. If one end of a channel disconnects, it automatically disconnects the other end too. This disconnection part has no effect with UDP. Automatically sets -m 10.
    #[value(name = "channels", alias = "c")]
    Channels,

    /// Lingering Channel mode: same as channels mode, but if one end of a channel disconnects, the other is left open.
    #[value(name = "linger-channels", alias = "lc")]
    LingerChannels,
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
#[command(
author,
version,
about,
long_about = None,
disable_help_flag = true,
override_usage =
r#"connect outbound: nc [options] HOST:PORT[xMult] [HOST:PORT[xMult] ...]
       listen for inbound: nc [[-l | -L] ADDR:PORT ...] [options]"#,
after_help =
r#"For -l, -L, and -s, a few formats of ADDR:PORT are supported:
- HOST:PORT - standard format, anything that can be parsed as a local address, including DNS lookup
- :PORT - automatically enumerates all local addresses
- *:PORT - uses the wildcard IPv4 and IPv6 addresses (0.0.0.0 and [::]) with the specified port
- * - same as above but implicitly use port 0"#
)]
pub struct NcArgs {
    /// this cruft (--help for long help)
    #[arg(short = 'h')]
    help: bool,

    #[arg(long = "help", hide = true)]
    help_more: bool,

    /// Use UDP instead of TCP
    #[arg(short = 'u')]
    is_udp: bool,

    /// Listen for incoming connections and exit after servicing first client. Can be specified multiple times to listen on different addresses. See notes below too.
    #[arg(short = 'l', value_name = "ADDR:PORT")]
    listen_once: Vec<String>,

    /// Listen repeatedly for incoming connections. Can be specified multiple times to listen on different addresses. See notes below too.
    #[arg(short = 'L', value_name = "ADDR:PORT", conflicts_with = "listen_once")]
    listen_many: Vec<String>,

    /// Max incoming clients allowed to be connected at the same time. (TCP only).
    #[arg(short = 'm', conflicts_with = "is_udp")]
    max_inbound_connections: Option<usize>,

    /// Source address to bind to for outbound connections
    #[arg(short = 's', value_name = "ADDR:PORT")]
    outbound_source_host_opt: Option<String>,

    /// Forwarding mode
    #[arg(short = 'f', long = "fm", value_enum, default_value_t = ForwardingMode::Null)]
    forwarding_mode: ForwardingMode,

    /// Should reconnect on graceful socket close.
    #[arg(short = 'r', requires = "targets", conflicts_with = "is_udp")]
    should_reconnect_on_graceful_close: bool,

    /// Should reconnect on socket error.
    #[arg(short = 'R', requires = "targets", conflicts_with = "is_udp")]
    should_reconnect_on_error: bool,

    /// Send size (affects input chunk size and UDP datagram size)
    #[arg(long = "ss", default_value_t = 1)]
    send_size: u32,

    /// Send buffer size
    #[arg(long = "sb")]
    sendbuf_size_opt: Option<u32>,

    /// Recv buffer size
    #[arg(long = "rb")]
    recvbuf_size_opt: Option<u32>,

    /// How long to delay between sends, in milliseconds.
    #[arg(
        long = "sd",
        alias = "send-delay",
        value_name = "MS",
        default_value_t = 0
    )]
    send_delay_ms: u64,

    /// How long to delay incoming receives, in milliseconds.
    #[arg(
        long = "rd",
        alias = "receive-delay",
        value_name = "MS",
        default_value_t = 0
    )]
    receive_delay_ms: u64,

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
    #[arg(short = 'i', value_enum, default_value_t = DEFAULT_INPUT_MODE)]
    input_mode: InputMode,

    /// Output mode
    #[arg(short = 'o', value_enum, default_value_t = OutputMode::Stdout)]
    output_mode: OutputMode,

    /// Exit after input completes (e.g. after stdin EOF)
    #[arg(long = "exit-after-input", alias = "eai")]
    should_exit_after_input_closed: bool,

    /// Connect local input and output to an executed program
    #[arg(short = 'e', long = "exec", value_name = "COMMAND")]
    exec_command_opt: Option<String>,

    /// Join multicast group given by hostname (outbound UDP only)
    #[arg(long = "mc", requires = "is_udp", requires = "targets")]
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

    /// Print ongoing throughput stats
    #[arg(long = "stats")]
    should_track_stats: bool,

    /// How often to send latency pings and print out throughput stats, in milliseconds.
    #[arg(
        long = "stats-time",
        alias = "st",
        value_name = "MS",
        default_value_t = 1000
    )]
    stats_period: u32,

    /// Emit verbose logging.
    #[arg(short = 'v')]
    verbose: bool,

    /// Host:Port pairs to connect to. Can optionally add e.g. x10, to connect to that target 10 times.
    #[arg(value_name = "HOST:PORT[xMULT]")]
    targets: Vec<String>,
}

impl NcArgs {
    fn is_listening(&self) -> bool {
        !self.listen_once.is_empty() || !self.listen_many.is_empty()
    }

    fn is_using_channels(&self) -> bool {
        match self.forwarding_mode {
            ForwardingMode::Channels | ForwardingMode::LingerChannels => true,
            _ => false,
        }
    }

    fn is_interactive_input_mode(&self) -> bool {
        // If the user is executing a child program, then the main stdin/stdout are not used, so it's definitely not
        // interactive.
        if self.exec_command_opt.is_some() {
            return false;
        }

        // Only the main Stdin mode (in particular not StdinNoCharMode) counts as interactive, but only if there's a
        // real terminal present. Something like file redirection wouldn't count.
        match self.input_mode {
            InputMode::Stdin => std::io::stdin().is_terminal(),
            _ => false,
        }
    }

    fn get_send_delay(&self) -> Duration {
        Duration::from_millis(self.send_delay_ms)
    }

    fn get_receive_delay(&self) -> Duration {
        Duration::from_millis(self.receive_delay_ms)
    }
}

fn usage(msg: &str) -> ! {
    eprintln!("Error: {}", msg);
    eprintln!();
    let _ = NcArgs::command().print_help();
    std::process::exit(1)
}

async fn async_main() -> Result<(), String> {
    let mut args = NcArgs::parse();

    if args.help_more {
        let _ = NcArgs::command().print_long_help();
        std::process::exit(1)
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

    match args.input_mode {
        InputMode::PfQosClient | InputMode::PfQosServer => {
            if !args.is_udp {
                usage("pfqos* input modes require UDP.");
            }
        }
        _ => {}
    }

    let mut targets: Vec<ConnectionTarget> = vec![];

    if !args.targets.is_empty() {
        eprintln!("Targets:");
        for target in args.targets.iter() {
            let mut target: &str = &target;
            let mut multiplier = 1;

            // Check and see if the user appended x123 or whatever as a multiplier at the end of the target string.
            if let Some(captures) = TARGET_MULTIPLIER_REGEX.captures_iter(&target).next() {
                // Capture 0 is the entire matched text, so the part from the start up to the first captured character
                // is the "before" portion.
                let before_match = &target[..captures.get(0).unwrap().start()];

                // Get the first capture, which should be the multiplier string.
                if let Some(multiplier_match) = captures.get(1) {
                    // Unwrap is OK here because the regex validated that this is a number only.
                    multiplier = multiplier_match.as_str().parse::<u32>().unwrap();
                    target = before_match;
                }
            }

            let ct = ConnectionTarget::new(target).await.map_err(format_io_err)?;
            if multiplier != 1 {
                eprintln!("{}x {}", multiplier, ct);
            } else {
                eprintln!("{}", ct);
            }

            for _ in 0..multiplier {
                targets.push(ct.clone());
            }
        }
    }

    if !args.is_listening() && targets.is_empty() {
        usage("Need host:port to connect to!");
    }

    // When joining a multicast group, by default you will send traffic to the group but won't receive it unless also
    // bound to the port you're sending to. If the user didn't explicitly choose a local port to bind to, choose the
    // outbound multicast port because it's probably what they actually wanted.
    if args.should_join_multicast_group && args.outbound_source_host_opt.is_none() {
        if let Some(first_target) = &targets.first() {
            if let Some(first_target_addr) = &first_target.addrs.iter().take(1).next() {
                args.outbound_source_host_opt = Some(format!("*:{}", first_target_addr.port()));
            }
        }
    }

    // Option::iter() makes an iterator that yields either 0 or 1 item, depending on if it's None or Some. The `true`
    // param for get_local_addrs tells it to automatically include the wildcard local address if no source addresses
    // were explicitly specified.
    let outbound_source_addrs = get_local_addrs(
        args.outbound_source_host_opt.iter().map(String::as_str),
        true,
        &args,
    )
    .await
    .map_err(format_io_err)?;

    // Should be handled by the conflicts_with attribute above.
    assert!(args.listen_once.is_empty() || args.listen_many.is_empty());

    let listen_addr_strings = if !args.listen_once.is_empty() {
        &args.listen_once
    } else {
        &args.listen_many
    };

    // If the user didn't pass `-l` or `-L`, then we shouldn't listen on any addresses. Pass `false` to get_local_addrs
    // to prevent it from automatically including the wildcard local addresses.
    let listen_addrs =
        get_local_addrs(listen_addr_strings.iter().map(String::as_str), false, &args)
            .await
            .map_err(format_io_err)?;

    // If max_inbound_connections wasn't specified explicitly, set its value automatically. If in hub or channel mode,
    // you generally want more than one incoming client at a time, or else why are you in a forwarding mode??
    // Otherwise, safely limit to just one per user-specified listen address at a time.
    if args.max_inbound_connections.is_none() {
        args.max_inbound_connections = Some(
            listen_addr_strings.len()
                * match args.forwarding_mode {
                    ForwardingMode::Null => 1,
                    ForwardingMode::Hub
                    | ForwardingMode::Channels
                    | ForwardingMode::LingerChannels => 10,
                },
        );
    }

    let result = if args.is_udp {
        do_udp(&listen_addrs, &outbound_source_addrs, &targets, &args).await
    } else {
        do_tcp(&listen_addrs, &outbound_source_addrs, &targets, &args).await
    };

    result.map_err(format_io_err)
}

fn main() -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let result = runtime.block_on(async_main());

    // At this point there may be a task blocked on reading from stdin or a child process's stdout. It won't return
    // until the next read completes, but we don't want to wait for that. Given that we've already decided not to do
    // anything further (the main task above is done), just shut down those lingering tasks so the program can exit.
    runtime.shutdown_background();

    result
}
