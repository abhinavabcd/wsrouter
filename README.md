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
3. `POST /renew` with `Authorization: Bearer TOKEN`. Any server accepts the request and proxies it to the node's current ring owner, which issues a fresh token for the same node ID and node type and disconnects all existing sockets for that node before returning success. Reconnect with the returned token. Without an external verifier, renew before expiry. Tokens carrying `auth_verify_url` can also be renewed after expiry if that verifier approves. The optional JSON body `{"valid_for":3600}` sets its new lifetime; the default is 90 days. Keep the newest token for reconnects.
4. Send JSON packets as WebSocket **text** messages. Regular nodes (`node_type` omitted or `"node"`) leave `src_id` and `x_src_id` out; the router fills them from the verified token and sets `src_node_id` to the authenticated node ID, replacing any supplied `src_node_id`. Supply `_id` if you already have one; otherwise the router assigns one.
5. On a `reconnect` packet, read its `payload` as the new server's HTTP base URL and connect there with the same token. The old connection closes after sending the packet.

The `type` field identifies the payload format so the receiver knows how to decode `payload`. Delivery feedback uses `type: "status"` and a string payload of `"success"` or `"failed"`; there is no separate `status` field. The `payload` field is always a string. For binary data, clients should base64-encode the bytes before sending and decode them after receiving. Use `dest_node_id` for one destination or `dest_node_ids` for 1 to 1,024 destinations; do not include both. Example client packet:

```json
{"type":"data","dest_node_id":"0123456789abcdef0123456789abcdef","payload":"AP8="}
```

To send the same payload to multiple nodes:

```json
{"type":"data","dest_node_ids":["0123456789abcdef0123456789abcdef","11111111111111111111111111111111"],"payload":"AP8="}
```

The router sends a separate copy to each destination. Each delivered copy has its own `dest_node_id` and no `dest_node_ids` field. All copies share one `_id`, and the source receives a `status` for each destination; each status uses that destination as `src_node_id` and `src_id`.

Example delivered packet:

```json
{"_id":"a18b9f56c718c449-7","type":"data","dest_node_id":"0123456789abcdef0123456789abcdef","src_id":"fedcba9876543210fedcba9876543210","src_node_id":"fedcba9876543210fedcba9876543210","x_src_id":"account-42","payload":"AP8="}
```

The server assigns `_id` only when it is absent or empty, and preserves a supplied value during forwarding and replay.

Messages also carry `timestamp`, an integer Unix time in milliseconds. For regular node messages, the router sets this to its receive time, replacing any supplied value. For server-token messages and peer forwarding, an existing timestamp (including zero) is preserved; a missing or null timestamp is filled with the current time. Fanout copies and replays retain the original timestamp. Router-generated feedback carries its own creation timestamp.

The router sends feedback to the source using the same ring:

```json
{"_id":"a18b9f56c718c449-7","type":"status","dest_node_id":"fedcba9876543210fedcba9876543210","src_id":"0123456789abcdef0123456789abcdef","src_node_id":"0123456789abcdef0123456789abcdef","payload":"success"}
```

`success` means at least one destination connection accepted the packet into its send queue. An offline destination or a forwarding failure returns `type: "status"`, `payload: "failed"`, and the original `_id`. On an unexpected socket close, packets in that socket's `tcp_keep_alive` window are rerouted first with the same `_id`. If a connection is available at the current owner, replay produces `success`; otherwise it produces `failed`. The server also keeps receipts for 10 seconds for packets outside the replay window; those produce `failed` only if this server still owns the node and no destination connections remain. A packet retried from the replay window does not also get a receipt-based failure. `success` is therefore an enqueue notice, not a final delivery guarantee. A client may receive another `success` if a recent packet is replayed after a connection drops. Feedback identifies the original destination in `src_node_id` (also in `src_id` for compatibility); use `src_node_id` together with `_id` to correlate each destination's result. Regular clients cannot send `status`.

Example failure feedback:

```json
{"_id":"a18b9f56c718c449-7","type":"status","dest_node_id":"fedcba9876543210fedcba9876543210","src_id":"0123456789abcdef0123456789abcdef","src_node_id":"0123456789abcdef0123456789abcdef","payload":"failed"}
```

WebSocket origins from any domain are accepted. HTTP routes send `Access-Control-Allow-Origin: *` and answer CORS preflight requests. The service uses bearer tokens, not cookies. `/create` accepts the caller's `x_src_id` as supplied; if that value represents a trusted account, authenticate callers at a gateway.

### External authorization and expired-token renewal

Add `auth_verify_url` to the existing `POST /create` request (`/generate` remains removed):

```json
{"x_src_id":"account-42","auth_verify_url":"https://example.com/account-42/verify","valid_for":3600}
```

The URL becomes part of the HMAC-signed token claims. Before each new WebSocket connection, the router makes a GET request to that URL. HTTP `200` with a body of `ok` allows authentication; matching is case insensitive and surrounding whitespace is ignored, so `OK` also works. Any other response, redirect, network error, or timeout denies authentication with HTTP `401`. Requests have a five-second timeout. `/locate` applies the same check. Tokens without this field use the existing local signature and expiry checks without an outbound request.

On every `/renew` request for a token carrying this field, the node's current ring owner calls the same URL with `action=renew`, for example `https://example.com/account-42/verify?action=renew`. Other query parameters are preserved. An approving response permits renewal even after the token expires, provided its signature and issuer key still verify. A denied response blocks renewal even before expiry and leaves existing sockets connected. Renewal preserves `auth_verify_url`, node identity, and node type; it cannot remove or replace the verifier. The URL must use HTTP or HTTPS and contain no embedded credentials or fragment. The router sends no bearer token to the verifier, so encode the account or authorization reference into the URL itself.

Servers proxy renewal request bodies, authorization, response status, and response headers to and from the owner. Only the owner performs external authorization. Forwarding has a ten-second timeout and a four-hop limit for inconsistent ring snapshots. Successful renewal closes the node's current sockets, including server-token sockets; unrelated nodes and cluster peer links stay connected. Older signed tokens retain their normal validity, so revoking future reconnects still requires expiry or denial by `auth_verify_url`.

Changing the verifier response revokes future authentication and renewal; it does not close sockets that are already open. Verifier checks are not cached.

For a trusted server connection, call `POST /create` with `{"x_src_id":"router-name","node_type":"server"}` and an `X-Cluster-Signature` header containing the hex HMAC-SHA256 of the exact request body with `SERVER_SECRET`. Server tokens can send packets with their own `src_id`, `src_node_id`, `x_src_id`, and `_id`; the router preserves supplied values while forwarding, including `status` packets. `src_node_id` identifies the sender for routing feedback; older server messages may provide `src_id` instead. Server-provided identity fields pass through unchanged. A missing `_id` is generated. The server-token request requires the cluster signature because these connections may send on behalf of other nodes.

## Routing

A server hashes the UTF-8 node ID with SHA-256, uses the first 63 bits as a ring position, and chooses the first server at or after it, wrapping around. A packet is sent to every active connection for its destination node. Servers forward packets over persistent `/peer` WebSockets authenticated with HMAC-SHA256 using the sending server's generated key. These links ping every 3 seconds, keep the last 3 seconds of packets sent and received, and reroute those packets when a link drops. The same `_id` is preserved. Membership heartbeats occur every minute; membership changes are pushed immediately, and a server is removed after three minutes without a heartbeat. `GET /health` reports the current ring size, number of distinct local node IDs, and counts of data routing errors and status messages that could not be routed.

Packets are limited to 1 MiB. Each client connection has a bounded 4,096-packet send queue and a 1,024-packet routing queue capped at 4 MiB of pending input; clients that overrun either queue are disconnected. Routing runs separately from the WebSocket reader so ping and pong frames remain responsive during forwarding. Servers send client WebSocket pings every `TCP_KEEP_ALIVE` seconds (default `30`; set it as a positive-seconds environment variable). A connection may override this with `tcp_keep_alive=SECONDS` on `/connect`; trusted server-token connections default to 3 seconds. The server retains packets sent toward each client socket for at least its keepalive interval and reroutes recent packets if that socket closes unexpectedly. A connection that does not answer a ping closes after a further ten seconds. With a longer ping interval, detection of a dropped connection can take the interval plus ten seconds. Each node has its own connection and receipt lock, so packet delivery to different nodes can run concurrently. One server-wide cleanup ticker scans only active replay histories and nodes with recent receipts; idle nodes have no receipt timer or cleanup work. Replay attempts are capped at three per packet to avoid loops. Delivery and replay are best effort and may produce duplicates with the same `_id`; callers needing exactly-once handling should deduplicate. A server process crash loses its in-memory queues, so it cannot replay packets or emit later `failed` notices for them. Membership changes can briefly interrupt delivery. Keys generated before a server restart are lost, so tokens issued by that server can stop verifying after its old membership entry is replaced. If node identity must survive an issuer restart, the tokens must first be renewed under a different current ring owner; contacting another server alone proxies renewal back to the owner.

The first server and full membership snapshots are practical limits on cluster size. This design scales across servers but is neither literally infinite nor coordinator fault tolerant.

## Tests

```sh
go test -race ./...
WSROUTER_STRESS=1 go test -run TestStressFiveServers -count=1 -v -timeout=10m
```

The opt-in stress test starts five servers and creates 1,000 clients total, 200 through each server. Half the clients send one batch to the other 999 destinations in random order; the other half send individual packets. The test waits for all 999,000 data packets and 999,000 `success` statuses, checking sender identity and payload while deduplicating replayed packets by their preserved `_id`. It advances receipt expiry in the test harness before disconnecting all clients, so teardown does not generate another million expected `failed` notices. For a smaller local smoke run, set `WSROUTER_STRESS_NODES=50` alongside `WSROUTER_STRESS=1`.
