## Intro to Kylin 5

### Comparison with Kylin 4.0

- New metadata design [New metadata design article (Chinese ver)](https://kylin.apache.org/5.0/blog/introduction_of_metadata_cn) and [New metadata definition](https://github.com/apache/kylin/blob/doc5.0/website/blog/2022-12-18-Introduction_of_Metadata/protocol-buffer/metadata.proto)
- Support Table Index
- Support schema change
- Support computed column
- New CuboidScheduler
- New Job engine etc.

For more detail, please check our [roadmap](https://kylin.apache.org/5.0/docs/development/roadmap) .

### Quick Start

1. Build maven artifact with following command:
```shell
mvn clean package -DskipTests
```

2. Run unit test with following command:

```shell
sh dev-support/unit_testing.sh
```

3. Build a Kylin 5 binary

```shell
./build/release/release.sh
```
