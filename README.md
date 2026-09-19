# hive-datascript

DataScript (in-memory) implementation of the hive-spi swarm store ports:
`ISwarmRegistry`, `IClaimStore`, `ICriticalOps`, `ICoordination` and `ISwarmDb`
(`hive-spi.swarm.protocol`).

It is a library, not an addon. The host constructs the records and installs
them into the hive-spi slots; thresholds and session scoping are injected by
the host. It depends only on hive-spi, datascript and leaf libraries, never on
hive-agent or hive-mcp.

MIT licensed.
