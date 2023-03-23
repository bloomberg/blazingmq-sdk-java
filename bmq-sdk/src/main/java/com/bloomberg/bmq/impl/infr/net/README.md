# `netty` Notes

Some miscellaneous notes on `netty`, which is an async event-driven network
library.

## Links

1. Docs: http://netty.io

2. Repo: https://github.com/netty/netty

3. `EchoClient` example: https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/echo

4. More client examples: https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example

## Channel-related

1. Multiple [ChannelHandler](https://netty.io/4.1/api/io/netty/channel/ChannelHandler.html)'s
   can be associated with a channel.  List of handlers is maintained by the
   [ChannelPipeline](https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html).

2. Channel's context is maintained in a
   [ChannelHandlerContext](https://netty.io/4.1/api/io/netty/channel/ChannelHandlerContext.html)

3. A `ChannelHandler` handles an I/O event or intercepts an I/O operation, and
   forwards it to the next handler in its `ChannelPipeline`. A `ChannelHandler`
   can be
   [ChannelInboundHandler](https://netty.io/4.1/api/io/netty/channel/ChannelInboundHandler.html)
   or
   [ChannelOutboundHandler](https://netty.io/4.1/api/io/netty/channel/ChannelOutboundHandler.html)
   or both

4. [ChannelHandlerAdapter](https://netty.io/4.1/api/io/netty/channel/ChannelHandlerAdapter.html)
   provides skeletal impl of `ChannelHandler` for convenience.

5. `ChannelHandlerAdapter` itself can be of 2 types:
   [ChannelInboundHandlerAdapter](https://netty.io/4.1/api/io/netty/channel/ChannelInboundHandlerAdapter.html)
   and
   [ChannelOutHandlerAdapter](https://netty.io/4.1/api/io/netty/channel/ChannelOutboundHandlerAdapter.html).
   There is also
   [ChannelDuplexHandler](https://netty.io/4.1/api/io/netty/channel/ChannelDuplexHandler.html)
   which represents a combination out of `ChannelInboundHandler` and the
   `ChannelOutboundHandler`.  It is a good starting point if application's
   `ChannelHandler` implementation needs to intercept operations and also state
   updates.

6. [ReadTimeoutHandler](https://netty.io/4.1/api/io/netty/handler/timeout/ReadTimeoutHandler.html)
   can be registered with the `ChannelPipeline` for timed-read operations,
   which are useful during negotiation.  It is most likely not suitable to be
   used in conjuction with other timed operations like *open-queue*,
   *configure-queue* etc.

## EventLoop-related

1. [Bootstrap](https://netty.io/4.1/api/io/netty/bootstrap/Bootstrap.html) can
   be used to bootstrap a client.

2. `Bootstrap` instance needs to be registered with an event loop instance
   which will be used to handle all events associated with the channels which
   will be created (see *(4)* below for more).

3. `Bootstrap` instance also needs to be registered with a
   [ChannelInitializer](https://netty.io/4.1/api/io/netty/channel/ChannelInitializer.html)
   which can be used to initialize a channel.  A common step when initializing
   a channel is to register a `ChannelHandler` in the newly-created `Channel`'s
   `ChannelPipeline`.

4. [NioEventLoopGroup](https://netty.io/4.1/api/io/netty/channel/nio/NioEventLoopGroup.html)
   is used for NIO selectable channels.  This class supports creating
   multiple I/O threads (it extends [MultithreadEventLoopGroup](https://netty.io/4.1/api/io/netty/channel/MultithreadEventLoopGroup.html)),
   but number of threads can be controlled by
   [this](https://netty.io/4.1/api/io/netty/channel/nio/NioEventLoopGroup.html#NioEventLoopGroup-int-)
   flavor of its constructor.  **NOTE**: passing a value of zero or using
   [this](https://netty.io/4.1/api/io/netty/channel/nio/NioEventLoopGroup.html#NioEventLoopGroup--)
   flavor of the constructor leads indicates `netty` to create two-times the
   number of CPUs on the box! (see
   [here](https://github.com/netty/netty/blob/4.1/transport/src/main/java/io/netty/channel/MultithreadEventLoopGroup.java#L40)).
   There is also [NioEventLoop](https://netty.io/4.1/api/io/netty/channel/nio/NioEventLoop.html)
   which claims to be a single-threaded event loop, but its constructor (which
   is not documented btw!) takes a bunch of odd arguments.

## TCP Flow Control and HWM

1. [ChannelOption.WRITE_BUFFER_WATER_MARK](https://netty.io/4.1/api/io/netty/channel/ChannelOption.html#WRITE_BUFFER_WATER_MARK)
   can be used to set high and low water marks on the outbound buffer of the
   channel like so: `d_boostrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(100, 200))`

2. [Channel.isWritable()](https://netty.io/4.1/api/io/netty/channel/Channel.html#isWritable--)
   can be invoked to query current writability of the channel.

3. [ChannelInboundHandler.channelWritabilityChanged](https://netty.io/4.1/api/io/netty/channel/ChannelInboundHandler.html#channelWritabilityChanged-io.netty.channel.ChannelHandlerContext-_)
   is fired whenever channel's writability changes.  This can be used to
   unblock writer thread if desired.

## Threading Model

1. All events are fired in the *IO* thread.

2. `Channel.write` is *always* an async operation.  It simply enqueues the
   buffer to the outbound buffer and returns.

3. `Channel.flush` can be invoked to write outstanding buffer over the wire.
   `Channel.flush` is also async.. it simply enqueues a request to flush in the
   *IO* thread.

## Channel Connection Retries

See `doConnect` and other functions in
[this](https://gist.github.com/ochinchina/e97606fd0b15f106c91c) snippet.

### Initial Connection Retries

1. In sync mode, this is simple -- simply invoke `NettyTcpChannel.connect()`
   specified number of times after specified number of intervals.

2. In async mode, initial retries can be made by overriding
   [ChannelFutureListener.operationComplete](https://netty.io/4.1/api/io/netty/channel/ChannelFutureListener.html).
   on the `ChannelFuture` returned by `Bootstrap.connect` if `ChannelFuture`
   is not a success.

### Subsequent connection retries

1. Upon getting a successful `Channel`, a `ChannelFutureListener` can be
   registered with `Channel.closeFuture`, and its `operationComplete` can be
   overriden to schedule a `Bootstrap.connect` via `EventLoop.schedule` after a
   delay.  **NOTE** when peer's channel goes down, both `channelInactive` as
   well as `ChannelFutureListener.operationComplete` installed with the close
   future are fired.  `operationComplete` is fired first, followed by
   `channelInactive`.

2. Another option is to schedule `Bootstrap.connect` in
   `ChannelInboundHandler.channelInactive`.

