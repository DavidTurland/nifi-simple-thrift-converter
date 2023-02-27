<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Java Example](#java-example)
  - [Generate thrift, run client unit tests](#generate-thrift-run-client-unit-tests)
  - [Build and run benchmark](#build-and-run-benchmark)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Java Example
## Generate thrift, run client unit tests
```bash
bash build.bash
```

## Build and run benchmark
```bash
mvn clean verify
java -jar target/benchmarks.jar
```