# netcrab

> A multi-purpose TCP/UDP networking tool

## Overview

Netcrab is a command-line networking tool that can do a lot of things. It is an homage to [netcat](https://nmap.org/ncat/) and can do many of the things it can. A brief overview of the things it can do:

- send TCP traffic in client or server mode
- send and receive UDP datagrams
- act as a broadcast router between multiple peers
- act as a connection forwarder to get around firewall restrictions

For full usage, run `netcrab --help`.

## TCP client

`netcrab HOST:PORT`

This connects to the specified TCP server. Now your stdin will be sent to the server and stdout will be printed out.

The IPv6 format for the hostname should use surrounding square brackets, for example localhost is `[::1]`.

## TCP server

`netcrab -L ADDR:PORT`

`netcrab -l ADDR:PORT`

You can listen as a TCP server on one or more arbitrary ports. Using `-l` exits the program after the first incoming connection disconnects. Use `-L` to "listen harder": continue listening after the disconnection.

The `ADDR:PORT` syntax supports some special variants:
- *HOST*:*PORT* - standard format, anything that can be parsed as a local address, including DNS lookup. E.g. `localhost:5000`
- :*PORT* - automatically enumerates all local addresses. E.g. `:5000`
- \*:*PORT* - uses the wildcard IPv4 and IPv6 addresses (0.0.0.0 and [::]) with the specified port. E.g. `*:5000`
- \* - same as above but implicitly use port 0

The TCP server by default only allows a single incoming connection to be active at a time per listening address, but the `-m max_inbound_clients` flag allows more than one to connect.

As with client mode, stdin is sent to all connected sockets, and incoming data from all sockets is sent to stdout. This can be changed using the input and output mode arguments below.

The `-z` argument causes the socket to immediately disconnect without allowing sending any data. Useful for just testing connectivity.

## UDP endpoint

`netcrab -u -L ADDR:PORT`

`netcrab -u HOST:PORT`

UDP is weird in that you don't really "connect" with it. You bind to a local port and then send/receive datagrams to/from remote peers. Listening mode and connecting mode for UDP work exactly the same, except that in connecting mode, the first peer to send stdin to is known at the start, and in listening mode, stdin traffic can't be sent anywhere until the listener receives at least one datagram from a peer.

For `-L` this supports the same `ADDR:PORT` syntax as above for TCP.

Datagram size defaults to 1 byte but can be controlled by the `--sb` argument.

## Listening on multiple local sockets

`netcrab -L ADDR1:PORT1 -L ADDR2:PORT2`

`netcrab -u -L ADDR1:PORT1 -L ADDR2:PORT2`

Netcrab supports listening on multiple local addresses and ports at the same time. It will accept connections that arrive on any of them. This supports TCP and UDP.

## UDP Multicast support

`netcrab -u --mc HOST:PORT`

Netcrab supports joining UDP sockets to multicast groups by adding the `--mc` argument. It also gives controls for the TTL for multicast packets (`--ttl`) and whether to receive multicast packets looped back since the program is joined to the group (`--mc_no_loop`).

## UDP Broadcast support

`netcrab -u -b HOST:PORT`

Netcrab supports sending UDP broadcast datagrams.

## Input mode

`netcrab -i MODE`

The input mode can be controlled. By default, input comes from stdin. It accepts the following other modes:
- `none`: no input is possible. The only traffic that will be processed will be from remote peers.
- `stdin-nochar`: works the same as stdin, but doesn't engage "character mode". This works worse for interactive uses.
- `echo`: any received traffic will be echoed back to its sender.
- `rand`: random data will be generated. The random sizes of data can be controlled by `--rsizemin` and `--rsizemax`, and the type of data can be controlled by `--rvals`.
- `fixed`: only really useful for perf testing. The same fixed-size message with fixed data will be sent infinitely. You can control the size using `--sb` and the type of random data in it with `--rvals`

When using one of the stdin modes as input, you can specify `--exit-after-input` to quit the program after the input stream reaches end of file.

## Output mode

`netcrab -o MODE`

By default, output goes to stdout, but it's often useful to change it to `-o none` to skip all output. This is especially helpful when passing a large amount of traffic, since it can slow down doing the output.

## IO redirection

The default mode of Netcrab is to use stdin and stdout, so you can redirect input from a file and send output to a file (or piped between programs).

`netcrab HOST:PORT < file`

`echo message | netcrab HOST:PORT`

## Controlling address families

`netcrab -6`

`netcrab -4`

You can restrict to using only IPv6 or IPv4 address families. This makes more of a difference when connecting to hostnames that go through DNS resolution or when listening without specifying an explicit source address.

## Controlling source address for outbound connections

`netcrab -s ADDR:PORT`

When making an outbound TCP connection or sending UDP datagrams, by default Netcrab binds to the wildcard IPv4 and IPv6 addresses (0.0.0.0:0 and [::]:0). You can pass `-s` to explicitly bind to an address instead. This supports all the `ADDR:PORT` variants described in the "TCP Server" section.

## Connecting to multiple outbound targets

`netcrab HOST1:PORT1 HOST2:PORT2`

Netcrab allows connecting to more than one remote peer at the same time. Similarly to listening for multiple concurrent connections, traffic from the local machine will be sent to all connected peers, inbound or outbound.

## Listening and connecting at the same time

`netcrab -L ADDR:PORT HOST:PORT`

Netcrab supports both listening for inbound connections and making outbound connections at the same time. This could be useful for proxying traffic from one local address to another, since you can independently specify both the address to listen on and the source address to use for the outbound connection.

## Multiple connections to an outbound target

`netcrab HOST:PORTxNUM`

Sometimes it's useful to be able to connect to the same endpoint multiple times, especially for channels scenarios. For example, to connect to a hostname 12 times, you could do `netcrab localhost:5000x12`. It will attempt 12 concurrent outbound connections to the same hostname and port.

This can also be combined with multiple targets. Here we connect to localhost on IPv4 five times, and on IPv6 thirteen times. `netcrab 127.0.0.1:5000x5 [::1]:5000x13`.

## Reconnecting

`netcrab -r`

`netcrab -R`

In outbound connection mode, you can ask Netcrab to re-establish a dropped connection. `-r` re-establishes on graceful connection close. `-R` re-establishes on ungraceful error. Both can be specified at the same time, either `-r -R` or `-rR`.

## Channels mode

`netcrab --fm channels`

`netcrab --fm linger-channels`

Now we get to the really useful parts. Netcrab can turn into a router, forwarding traffic between multiple endpoints. "Channels" mode has it pair up endpoints and forward traffic bidirectionally from one endpoint to the other, acting like a transparent proxy. Traffic is not forwarded between channels. Imagine the following diagram:

```
+-------------+    +-----------------------------+    +-------------+
|             |    |           netcrab           |    |             |
| HOST1:PORT1 <----> HOST2:PORT1 <-> HOST2:PORT2 <----> HOST3:PORT1 |
| HOST1:PORT2 <----> HOST2:PORT3 <-> HOST2:PORT4 <----> HOST3:PORT2 |
|             |    |                             |    |             |
+-------------+    +-----------------------------+    +-------------+
```

The first and second channels pass all the way through but don't cross the streams. If Host 1 or Host 3 disconnects an endpoint, the disconnection is "forwarded" to the other end of the channel too. If that behavior doesn't work for you, you can switch to Lingering Channels mode.

When in channels mode, max clients is automatically bumped to 10 per listening address under the assumption that the user probably wants more than one connection at a time in order to actually, you know, use the channels. This can be overridden with `-m`.

## Hub mode

`netcrab --fm hub`

Hub mode is similar to channels mode, but simpler: all traffic from all network sources is forwarded back to all other sockets. You could use it to set up a chat room or something.

As in channels mode, when in hub mode, max clients is automatically bumped to 10 per listening address but can be overridden with `-m`.

## Endless possibilities

Pretty much all of the capabilities described above can be combined and used at the same time. For example, you can listen on multiple addresses and connect to multiple targets in the same session while using hub to forward between all the connections.
