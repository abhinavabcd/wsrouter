# wsrouter

A Go WebSocket router with a consistent hash ring. All production code is in `main.go`. Authentication uses HTTPS verifiers, and every node ID contains its authentication domain. There is no shared signing secret or locally signed token.

## Run

All advertised addresses, verifier requests, and peer connections use HTTPS/WSS with certificate verification enabled. The listening port comes from `--addr`, defaulting to 443. Omitting `--clusteraddr` starts a leader. Give servers unique, well-spaced ring positions between 0 and 9223372036854775807.

```sh
go run . --addr=https://sub1.example.com:8443 --ssl_certs=/path/to/certs --id=0
go run . --addr=https://sub2.example.com:8443 --ssl_certs=/path/to/certs \
  --clusteraddr=https://sub1.example.com:8443 --id=4611686018427387904
```

The certificate directory should contain a matching `.pem` certificate chain and `.key` private key. `fullchain.pem`/`privkey.pem` and `public.pem`/`private.pem` are also supported. Each server must trust the certificates used by its peers and authentication authorities. See [local HTTPS testing](DOMAIN_AUTH.md#local-https-tests) for mkcert and automated test fixtures. The container image installs system CA certificates; a local CA must also be trusted inside the container.

## Create and connect

`POST /create` accepts these required fields:

```json
{"auth_domain":"auth.example.com","path":"/verify","secret_string":"opaque-secret"}
```

Example response:

```json
{"node_id":"auth.example.com|0123456789abcdef0123456789abcdef","token":"/verify|opaque-secret","address":"https://sub1.example.com:8443"}
```

`auth_domain` is a hostname with an optional port, without a scheme or path. `path` starts with `/` and can include query parameters. The first `|` separates the path from the secret, so the secret may contain additional `|` characters. Creation constructs the credentials; authentication occurs when they are used. The external authority must already recognize the supplied secret and decide which node IDs it permits.

Connect to `/connect` using URL-encoded `node_id` and `token` query parameters. Non-browser clients can instead send `X-Node-ID` and `Authorization: Bearer TOKEN`. Both identity and token are required. The router sends:

```http
POST https://auth.example.com/verify?node_id=auth.example.com%7C0123456789abcdef0123456789abcdef
Content-Type: application/json

{"node_id":"auth.example.com|0123456789abcdef0123456789abcdef","secret":"opaque-secret"}
```

The verifier must return HTTP 200 with JSON:

```json
{"valid":"true","rp5":10,"maxc":5}
```

Boolean `true` is also accepted. `maxc` is an optional positive integer, defaulting to 5, that limits tracked `/connect` sockets per node. A new connection evicts and closes the oldest tracked socket when the limit is reached. A lowered limit evicts as many oldest sockets as needed before tracking the new one. Eviction is an intentional close and does not replay recent packets. `rp5` is required and must be a positive integer. Plain `ok`, false/missing validity, invalid rates, redirects, certificate failures, and timeouts deny authentication. Verification has a five-second timeout and an 8 KiB response limit. The `node_id` query parameter always comes from the requested identity, replacing any existing values. Other query parameters are preserved, except `action`, which is controlled by the router.

The verifier can additionally return `x_src_id` and `expires` (Unix seconds). Without them, `x_src_id` defaults to the node ID and validity is decided by the authority on each authentication. A returned expiry must be in the future. `/create` no longer accepts trusted identity, rate, or lifetime claims from the caller: the authority supplies them. Old HMAC tokens are incompatible with this protocol.

`GET /locate` takes the same credentials, verifies them, and returns the ring owner's `address`. `/connect` returns HTTP 421 and `{"address":"https://owner:port"}` if the node belongs elsewhere. Browser WebSocket APIs cannot read that response body, so browser clients should use `/locate` before connecting. WebSocket origins from any domain are accepted. `/create` and `/connect` (along with the other HTTP endpoints) return `Access-Control-Allow-Origin: *` and answer CORS preflight requests. If an incoming request includes `Origin`, the router forwards it unchanged as the `Origin` header on its HTTPS verification request, including lookup and renewal. The authority can approve or deny that origin through its usual `valid` response. Requests without `Origin` leave it absent. `/create` constructs credentials without a verifier call; origin policy is checked when the credentials are used.

## Renewal

`POST /renew` takes the same node ID and token. The receiving server calls the verifier directly with `action=renew`, without looking up or proxying to the ring owner. Only an approving JSON response allows renewal. That server then disconnects any local sockets for the node and returns its identity, token, and verifier-provided policy. Sockets on other servers remain connected. Denied renewal leaves existing sockets connected.

An authority can return `secret_string` in its renewal response to rotate the credential. The router returns the existing path combined with that new secret. Without a replacement secret, the token stays the same. The authority must recognize the new secret and revoke the old one when rotation is intended; the router cannot mint credentials for an external backend. A verifier may approve renewal after its own expiry policy has denied normal connections, returning a fresh future `expires` value if it uses that field.

## Cluster authentication

Each process generates a local random secret and uses `advertised-host[:port]|ring-position` as its node ID and `/server|local-secret` as its token. A follower POSTs `server_domain`, `id`, and `secret` to the primary's `/server` endpoint. The primary checks the domain relationship and calls the joining server's HTTPS `/server?node_id=...` endpoint with JSON containing `node_id` and `secret`. That endpoint approves only the process's own identity and secret, using the same JSON response format. Registration also runs during the minute membership refresh, allowing a restarted process to register its new credential.

An authenticated authority matching the server's hostname or permitted parent domain identifies a server node. For `sub1.example.com`, the parent is `example.com`; for `sub1.x.y.com`, it is `x.y.com`. Siblings and descendants of that parent are trusted after HTTPS verification. Public suffixes such as `co.uk` and `github.io` are excluded using the public suffix list. IP addresses and `localhost` use exact hostname matching; ports do not change domain ownership.

The primary distributes server credentials in HTTPS ring snapshots. `GET /snapshot` from the TCP source `127.0.0.1` returns an unauthenticated text table (`addr`, `nodes`, `ringid`) for local inspection, without credentials in the output. Forwarded IP headers do not grant access. The local node count is live; remote counts reflect the last ring refresh. `POST /snapshot` updates still authenticate the configured primary, and `/peer` WebSocket upgrades require a registered server ID, matching credential, and successful HTTPS verification. Membership changes are pushed immediately; epochs and versions order updates. Servers disappear from the ring after three minutes without refresh. The primary must remain available for joins and ring refreshes.

Server nodes can supply their own `src_id`, `src_node_id`, `x_src_id`, timestamp, and status messages. Those fields pass through unchanged. Regular nodes have source identity assigned by the router and cannot send `status` packets.

After replacing certificate files, `POST /reload` on that server with its own server node ID in `X-Node-ID` and its local server token in `Authorization: Bearer TOKEN`. A successful reload returns 204 and updates new TLS connections; existing sockets remain open. Invalid files return 502 and retain the previous certificate. These administrative credentials are available to trusted cluster members through the ring snapshot; they are never logged or exposed by `/health`.

## Messages and delivery

Send JSON text frames with string payloads. Base64-encode binary data when needed:

```json
{"_id":"message-1","type":"data","dest_node_id":"auth.example.com|destination-id","payload":"AP8="}
```

Use `dest_node_ids` for fanout (1–1,024 destinations), or `dest_node_id` for one destination, but not both. Each copy has one `dest_node_id`, preserves the same `_id`, and routes to every connection for that destination. Missing or empty `_id` values are generated. Hashing includes the whole domain-prefixed node ID.

Regular nodes may provide an integer Unix millisecond `timestamp` between the server's receive time minus 8,000 milliseconds and its current time, inclusive. A missing/null timestamp is filled by the server. An invalid time produces `type: "drop", payload: "invalid_time"`, with the other input fields copied. Server nodes preserve supplied timestamps without drift checks. Replay preserves the original ID and timestamp.

The authority's `rp5` limits each regular connection using fixed server-time buckets (`UnixMilli / 5000`). One incoming packet consumes one allowance, including invalid timestamps and fanout requests. Excess traffic gets `type: "drop", payload: "rate_limited"`. Dropped packets are returned only to that socket and are not routed. Server nodes bypass these limits.

Delivery feedback uses the same ring:

```json
{"_id":"message-1","type":"status","payload":"success","dest_node_id":"auth.example.com|sender-id","src_node_id":"auth.example.com|destination-id","src_id":"auth.example.com|destination-id"}
```

`success` means a destination connection accepted the packet into its send queue. An unavailable destination or forwarding failure produces `payload: "failed"`. Feedback identifies the original receiver in `src_node_id` and legacy `src_id`, so correlate by `src_node_id` plus `_id`.

Servers ping client sockets every `TCP_KEEP_ALIVE` seconds (default 30), overridable through `tcp_keep_alive` on `/connect`. Server connections default to 3 seconds. A socket that does not answer closes after a further ten seconds. Packets sent toward a client socket during its keepalive window are rerouted on unexpected disconnect before deciding failure. Peer links retain the last three seconds of packets in both directions and replay them on disconnect. Replays preserve `_id` and stop after three attempts. Older receipts are retained for ten seconds and fail only when the destination remains locally owned and has no connections.

A shared cleanup job scans active histories and recent receipts; there is no timer per idle node. Replay is best effort and can create duplicates. Process crashes lose in-memory queues. `success` is an enqueue notice, not a final delivery guarantee.

Packets are limited to 1 MiB. Each client has a 4,096-packet send queue and a 1,024-packet routing queue capped at 4 MiB. Peer feedback has a separate prioritized backlog capped at 64 MiB; feedback enqueue never blocks routing workers. Overload failures appear in `/health` counters. `/health` also reports ring size and distinct local nodes.

The full ring snapshots and single membership coordinator impose practical scaling limits; this system is not infinitely scalable or coordinator fault tolerant.

## Tests

```sh
go test -race ./...
WSROUTER_STRESS=1 go test -run TestStressFiveServers -count=1 -v -timeout=10m
```

Tests use trusted local TLS certificates with hostname verification enabled. The stress test starts five HTTPS servers and 1,000 WSS clients, 200 created through each server. Every client sends to the other 999, mixing fanout and individual packets. It waits for 999,000 data packets and 999,000 success statuses, checking identity and deduplicating replay. The verifier supplies `rp5: 1000` for these clients. Set `WSROUTER_STRESS_NODES=50` for a smaller run.
