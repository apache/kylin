## Intro to Kylin 5

### Comparison with Kylin 4.0

- New metadata design [Metadata definition](document/protocol-buffer/metadata.proto)
- Support Table Index
- Support schema change
- Support computed column
- New CuboidScheduler
- New Job engine etc.

### Quick Start

1. Build maven artifact with following command:
```shell
mvn clean package
```

2. Run unit test with following command:

```shell
mvn clean test
```

3. Build a Kylin 5 binary

```shell
cd build/apache_release
./release.sh
```