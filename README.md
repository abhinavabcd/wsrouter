# wsrouter

A Go WebSocket router with a consistent hash ring. Clients have random `node_id` values; servers have fixed ring positions. The first server coordinates membership. Every server generates a fresh 32-byte authentication key at startup and sends it to the first server in its signed heartbeat. The first server includes all active server keys in signed snapshots and pushes a new snapshot to existing servers when membership changes. Snapshot epochs and versions keep concurrent updates in order.

## Run

Set the same `SERVER_SECRET` on every server (`server_secret` is also accepted). It authenticates membership updates and snapshots; each server's generated key signs the client tokens it issues and authenticates its peer WebSocket connections. Give servers unique, well-spaced positions between `0` and `9223372036854775807`.

```sh
export SERVER_SECRET='replace-with-at-least-32-random-bytes'
cd wsrouter
go run . --listen=:8080 --addr=http://localhost:8080 --clusteraddr=http://localhost:8080 --id=0
```

On a second host, with the same secret:

```sh
go run . --listen=:8081 --addr=http://localhost:8081 --clusteraddr=http://localhost:8080 --id=4611686018427387904
```

The advertised URLs must be reachable by clients and the other servers. Use HTTPS/WSS across machines because signed snapshots contain the per-server keys. The first server must remain available for joins and ring refreshes.

### Direct TLS and certificate reload

Pass `--ssl_certs` to serve HTTPS and WSS directly. Set `--addr` to an `https://` URL and provide a directory containing one certificate chain and its private key. Name them with the same stem, such as `server.pem` and `server.key`. The common `fullchain.pem`/`privkey.pem` and `public.pem`/`private.pem` pairs also work. The certificate must cover the advertised hostname and be trusted by clients and other servers.

```sh
go run . --listen=:8443 --addr=https://localhost:8443 --clusteraddr=https://localhost:8443 --ssl_certs=/path/to/ssl_certs --id=0
```

After replacing both certificate files, send a signed `POST /reload` to that server. A successful reload returns HTTP `204` and changes the certificate used for new TLS connections; existing WebSockets stay connected. An invalid pair returns HTTP `500` and leaves the previous certificate in use. The endpoint requires `X-Cluster-Signature`, the hex HMAC-SHA256 of `POST /reload` using `SERVER_SECRET`:

```sh
signature=$(printf 'POST /reload' | openssl dgst -sha256 -hmac "$SERVER_SECRET" | awk '{print $NF}')
curl -X POST -H "X-Cluster-Signature: $signature" https://localhost:8443/reload
```

Run the reload request on each server whose certificate files changed. Without `--ssl_certs`, the router continues to serve plain HTTP and `/reload` is unavailable.

## Client flow

1. `POST /create` with JSON `{"x_src_id":"your-client-id"}`. The response contains `node_id`, `node_type`, `token`, Unix `expires`, and the current owner `address` when the ring is available. Tokens identify the server that issued them. Add `"valid_for":3600` to set the lifetime in seconds; if omitted, the lifetime is 90 days (`7776000` seconds). `valid_for` must be positive.
2. Open `GET /connect?token=TOKEN` as a WebSocket, or send `Authorization: Bearer TOKEN` on the upgrade request. A wrong server returns HTTP `421` and JSON `{"address":"http://owner:port"}`. Retry at that address with the same token, converting `http` to `ws` or `https` to `wss`. Browser WebSocket APIs cannot read the `421` response body; browsers can call `GET /locate` with `Authorization: Bearer TOKEN` to obtain the current `address` via CORS before retrying.
3. Before expiry, `POST /renew` with `Authorization: Bearer TOKEN`. It returns a token for the same node ID and node type, signed by the server handling renewal. The optional JSON body `{"valid_for":3600}` sets its new lifetime; the default is 90 days. Keep the newest token for reconnects.
4. Send JSON packets as WebSocket **text** messages. Regular nodes (`node_type` omitted or `"node"`) leave `src_id` and `x_src_id` out; the router fills them from the verified token. Supply `_id` if you already have one; otherwise the router assigns one.
5. On a `reconnect` packet, read its `payload` as the new server's HTTP base URL and connect there with the same token. The old connection closes after sending the packet.

The `type` field identifies the payload format so the receiver knows how to decode `payload`. Delivery feedback uses `type: "status"` and a string payload of `"sent"` or `"failed"`; there is no separate `status` field. The `payload` field is always a string. For binary data, clients should base64-encode the bytes before sending and decode them after receiving. Use `dest_node_id` for one destination or `dest_node_ids` for 1 to 1,024 destinations; do not include both. Example client packet:

```json
{"type":"data","dest_node_id":"0123456789abcdef0123456789abcdef","payload":"AP8="}
```

To send the same payload to multiple nodes:

```json
{"type":"data","dest_node_ids":["0123456789abcdef0123456789abcdef","11111111111111111111111111111111"],"payload":"AP8="}
```

The router sends a separate copy to each destination. Each delivered copy has its own `dest_node_id` and no `dest_node_ids` field. All copies share one `_id`, and the source receives a `status` for each destination; each status uses that destination as `src_id`.

Example delivered packet:

```json
{"_id":"a18b9f56c718c449-7","type":"data","dest_node_id":"0123456789abcdef0123456789abcdef","src_id":"fedcba9876543210fedcba9876543210","x_src_id":"account-42","payload":"AP8="}
```

The server assigns `_id` only when it is absent or empty, and preserves a supplied value during forwarding and replay. It routes a feedback packet to the source using the same ring:

```json
{"_id":"a18b9f56c718c449-7","type":"status","dest_node_id":"fedcba9876543210fedcba9876543210","src_id":"0123456789abcdef0123456789abcdef","payload":"sent"}
```

`sent` means at least one destination connection accepted the packet into its send queue. An offline destination or a forwarding failure returns `type: "status"`, `payload: "failed"`, and the original `_id`. On an unexpected socket close, packets in that socket's `tcp_keep_alive` window are rerouted first with the same `_id`. If a connection is available at the current owner, replay produces `sent`; otherwise it produces `failed`. The server also keeps receipts for 10 seconds for packets outside the replay window; those produce `failed` only if this server still owns the node and no destination connections remain. A packet retried from the replay window does not also get a receipt-based failure. `sent` is therefore an enqueue notice, not a final delivery guarantee. A client may receive another `sent` if a recent packet is replayed after a connection drops. Feedback identifies the original destination in `src_id`; use that field together with `_id` to correlate each destination's result. Regular clients cannot send `status`.

Example failure feedback:

```json
{"_id":"a18b9f56c718c449-7","type":"status","dest_node_id":"fedcba9876543210fedcba9876543210","src_id":"0123456789abcdef0123456789abcdef","payload":"failed"}
```

WebSocket origins from any domain are accepted. HTTP routes send `Access-Control-Allow-Origin: *` and answer CORS preflight requests. The service uses bearer tokens, not cookies. `/create` accepts the caller's `x_src_id` as supplied; if that value represents a trusted account, authenticate callers at a gateway.

For a trusted server connection, call `POST /create` with `{"x_src_id":"router-name","node_type":"server"}` and an `X-Cluster-Signature` header containing the hex HMAC-SHA256 of the exact request body with `SERVER_SECRET`. Server tokens can send packets with their own `src_id`, `x_src_id`, and `_id`; the router preserves supplied values while forwarding, including `status` packets. `src_id` is required, and a missing `_id` is generated. The server-token request requires the cluster signature because these connections may send on behalf of other nodes.

## Routing

A server hashes the UTF-8 node ID with SHA-256, uses the first 63 bits as a ring position, and chooses the first server at or after it, wrapping around. A packet is sent to every active connection for its destination node. Servers forward packets over persistent `/peer` WebSockets authenticated with HMAC-SHA256 using the sending server's generated key. These links ping every 3 seconds, keep the last 3 seconds of packets sent and received, and reroute those packets when a link drops. The same `_id` is preserved. Membership heartbeats occur every minute; membership changes are pushed immediately, and a server is removed after three minutes without a heartbeat. `GET /health` reports the current ring size, number of distinct local node IDs, and counts of data routing errors and status messages that could not be routed.

Packets are limited to 1 MiB. Each client connection has a bounded 4,096-packet send queue and a 1,024-packet routing queue capped at 4 MiB of pending input; clients that overrun either queue are disconnected. Routing runs separately from the WebSocket reader so ping and pong frames remain responsive during forwarding. Servers send client WebSocket pings every `TCP_KEEP_ALIVE` seconds (default `30`; set it as a positive-seconds environment variable). A connection may override this with `tcp_keep_alive=SECONDS` on `/connect`; trusted server-token connections default to 3 seconds. The server retains packets sent toward each client socket for at least its keepalive interval and reroutes recent packets if that socket closes unexpectedly. A connection that does not answer a ping closes after a further ten seconds. With a longer ping interval, detection of a dropped connection can take the interval plus ten seconds. Each node has its own connection and receipt lock, so packet delivery to different nodes can run concurrently. One server-wide cleanup ticker scans only active replay histories and nodes with recent receipts; idle nodes have no receipt timer or cleanup work. Replay attempts are capped at three per packet to avoid loops. Delivery and replay are best effort and may produce duplicates with the same `_id`; callers needing exactly-once handling should deduplicate. A server process crash loses its in-memory queues, so it cannot replay packets or emit later `failed` notices for them. Membership changes can briefly interrupt delivery. Keys generated before a server restart are lost, so tokens issued by that server can stop verifying after its old membership entry is replaced. Renew those tokens on another active server before restarting the issuer if node identity must survive.

The first server and full membership snapshots are practical limits on cluster size. This design scales across servers but is neither literally infinite nor coordinator fault tolerant.

## Tests

```sh
go test -race ./...
WSROUTER_STRESS=1 go test -run TestStressFiveServers -count=1 -v -timeout=10m
```

The opt-in stress test starts five servers and creates 1,000 clients total, 200 through each server. Half the clients send one batch to the other 999 destinations in random order; the other half send individual packets. The test waits for all 999,000 data packets and 999,000 `sent` statuses, checking sender identity and payload while deduplicating replayed packets by their preserved `_id`. It advances receipt expiry in the test harness before disconnecting all clients, so teardown does not generate another million expected `failed` notices. For a smaller local smoke run, set `WSROUTER_STRESS_NODES=50` alongside `WSROUTER_STRESS=1`.
