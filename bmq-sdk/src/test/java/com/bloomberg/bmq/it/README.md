# Java BlazingMQ SDK integration tests

## Build and Run the Tests

See `Integration Tests` section in the root `README.md`.

## Tests description

- `BrokerSessionIT.java` tests `BrokerSession` start/stop and basic
  open/configure/close queue operations.

- `NettyConsumerIT.java` receives a payload message from the native broker
  using `BrokerSession` for connection and queue operations.

- `NettyProducerIT.java` sends a payload message to the native broker using
  `BrokerSession` for connection and queue operations.

- `NettyTcpConnectionImplIT.java` tests various tcp connectivity scenarios for
  `NettyTcpConnectionImpl`

- `PayloadIT.java` tests payload transfer from Java `Producer` to Java
  `Consumer` using native `Broker`.

- `PlainConsumerIT.java` receives a payload message from the native broker
  using raw socket. Lists consequent protocol stages needed to get a `PUSH`
  message.

- `PlainProducerIT.java` sends a payload message to the native broker using raw
  socket. Lists consequent protocol stages needed to send a `PUT` message.

- `TcpBrokerConnectionIT.java` tests session level connectivity like start/stop
  which involves `Negotiation` and `Disconnecting` stages together with related
  timeouts for `TcpBrokerConnection`.

## Test utils description

- `util/BmqBrokerSimulator`: `netty` based tcp server for testing purposes
  that runs in a separate thread within user process. Has several modes:
  - *SILENT_MODE*: lets the tcp connection be established, but never
    responds anything
  - *ECHO_MODE*: simply echoes whatever bytes are received
  - *BMQ_AUTO_MODE*: always respond with successful responses in BlazingMQ format
  - *BMQ_MANUAL_MODE*: user difines which BlazingMQ message should be sent in
    response
- `util/BmqBrokerTestServer`: utility class to start native BlazingMQ Broker in a
  child process.  To run the Broker locally `BMQ_BROKER_PATH` environment
  varible must be set to point to the Broker binary.
- `util/TestTcpServer.java` - common interface for the test servers.
- `util/TestTools.java` - helper utilities used in multiple tests.
