<p align="center">
  <a href="https://bloomberg.github.io/blazingmq">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="assets/images/blazingmq_logo_label_dark.svg">
      <img src="assets/images/blazingmq_logo_label.svg" width="70%">
    </picture>
  </a>
</p>

---

[![BlazingMQ](https://img.shields.io/badge/BlazingMQ-blue)](https://github.com/bloomberg/blazingmq)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue)](LICENSE)
[![Java](https://img.shields.io/badge/Java-blue)](#)
[![Maven](https://img.shields.io/maven-central/v/com.bloomberg.bmq/bmq-sdk)](https://mvnrepository.com/artifact/com.bloomberg.bmq/bmq-sdk)
[![MessageQueue](https://img.shields.io/badge/MessageQueue-blue)](#)
[![Documentation](https://img.shields.io/badge/Documentation-blue)](https://bloomberg.github.io/blazingmq)

# BlazingMQ Java SDK

This repository provides the official Java client library as well as examples
of how applications can interact with BlazingMQ.  BlazingMQ is an open source
message queue system with focus on efficiency, reliability and a rich feature
set.  Please see the BlazingMQ [repo](https://github.com/bloomberg/blazingmq)
and [documentation](https://bloomberg.github.io/blazingmq) for more details
about BlazingMQ.

This Java client is fully supported by the BlazingMQ team and we provide
feature and/or API parity with the BlazingMQ C++ client library.

## Menu

- [Using](#using)
- [Building](#building)
- [Contributions](#contributions)
- [License](#license)
- [Code of Conduct](#code-of-conduct)
- [Security Vulnerability Reporting](#security-vulnerability-reporting)

---

## Using

This repository contains two modules. The built packages for these modules are available in Maven Central:

- [bmq-sdk](https://central.sonatype.com/artifact/com.bloomberg.bmq/bmq-sdk): contains BlazingMQ Java client library code
- [bmq-examples](https://central.sonatype.com/artifact/com.bloomberg.bmq/bmq-examples): contains sample producer and consumer clients

## Building

### Build and Install the JARs

Execute from repo root:

```sh
$ mvn clean install
```

Above command will compile and install both modules -- `bmq-sdk` as well as
`bmq-examples`.  It will also run tests and carry out static analysis.  If it
is desired to skip running any tests and static analysis while creating the
JARs (because those steps were executed already), one can execute:

```sh
$ mvn clean -Dmaven.test.skip=true -Dspotbugs.skip=true install
```

### Build and Run `bmq-examples` Producer Example

```sh
$ cd bmq-examples
$ mvn clean compile
$ mvn exec:java -Dexec.mainClass="com.bloomberg.bmq.examples.Producer"
```

Above command expects that `bmq-sdk` JAR is installed locally.  Also note that
BlazingMQ backend must be running for producer/consumer examples to run
successfully.

### Building BlazingMQ Backend

Detailed instructions to build BlazingMQ backend (BlazingMQ message brokers,
etc) can be found [here](https://www.github.com/bloomberg/blazingmq).

### Supported JDKs

The SDK code supports building with the following kits:

- *JDK8*
  - Builds the code with these compiler parameters:
    - `source=8` (which allows up to *Java 8* features)
    - `target=8` (which generates JVM 8 bytecode)
- *JDK11* and *JDK17*
  - Builds the code with `release=8` (generated JVM 8 compatible code) plus
    builds the code in `java9` directory with `release=9`.  This way multi
    release JAR is produced which contains Java8 and Java9 versions of `Crc32c`
    class

By default, JDK defined in `JAVA_HOME` is used.  When running any maven
command, corresponding profile is activated depending on the JDK version.  To
get a list of active profiles run the following command:

```sh
# On this machine JAVA_HOME is set to JDK8.
$ mvn help:active-profiles
...
The following profiles are active:

 - JDK8 (source: com.bloomberg.bmq:bmq-sdk:X.Y.Z-SNAPSHOT)
```

To use another JDK, override `JAVA_HOME` env variable. For instance:

```sh
# clean output and compile the code using JDK 11
$ JAVA_HOME=${PATH_TO_JDK11} mvn clean compile
...
[INFO] BUILD SUCCESS
...
```

### Build and Run Unit Tests

- All unit tests

  ```sh
  $ mvn -q -Dspotbugs.skip=true -Dspotless.check.skip=true test  // '-q' for quiet mode
  ```
- A specific unit test class, e.g. `OpenQueueTest`

  ```sh
  $ mvn -q -Dspotbugs.skip=true -Dspotless.check.skip=true -Dtest=OpenQueueTest test
  ```
- A specific unit test method of a class, e.g. `OpenQueueTest.testReset`

  ```sh
  $ mvn -q -Dspotbugs.skip=true -Dspotless.check.skip=true -Dtest=OpenQueueTest#testReset test
  ```

---

## Contributions

We welcome your contributions to help us improve and extend this project!

We welcome issue reports [here](../../issues); be sure to choose the proper
issue template for your issue, so that we can be sure you're providing the
necessary information.

Before sending a [Pull Request](../../pulls), please make sure you read our
[Contribution
Guidelines](https://github.com/bloomberg/.github/blob/master/CONTRIBUTING.md).

---

## License

Please read the [LICENSE](LICENSE) file.

---

## Code of Conduct

This project has adopted a [Code of
Conduct](https://github.com/bloomberg/.github/blob/master/CODE_OF_CONDUCT.md).
If you have any concerns about the Code, or behavior which you have experienced
in the project, please contact us at opensource@bloomberg.net.

---

## Security Vulnerability Reporting

If you believe you have identified a security vulnerability in this project,
please send an email to the project team at opensource@bloomberg.net, detailing
the suspected issue and any methods you've found to reproduce it.

Please do NOT open an issue in the GitHub repository, as we'd prefer to keep
vulnerability reports private until we've had an opportunity to review and
address them.

---
