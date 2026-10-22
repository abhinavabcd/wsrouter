# HTTPS domain authentication migration

Implemented in `main.go`. Node connections, renewal, cluster registration, snapshots, and peer forwarding use the HTTPS authority protocol below. Shared-secret signing has been removed.

## Protocol

`POST /create` accepts:

```json
{"auth_domain":"auth.example.com","path":"/verify","secret_string":"opaque-secret"}
```

It returns `node_id: "auth.example.com|<random-id>"` and `token: "/verify|opaque-secret"`. The domain is a hostname with an optional port; it does not include a scheme or path. The verification path begins with `/` and may include query parameters. The token splits at its first `|`, allowing additional delimiters in the secret.

Clients supply both `node_id` and `token` to `/connect`, `/locate`, and `/renew`. Browser clients use URL-encoded query parameters. Other clients may use `X-Node-ID` and `Authorization: Bearer TOKEN` headers.

Authentication sends:

```http
POST https://auth.example.com/verify?node_id=auth.example.com%7C0123456789abcdef0123456789abcdef
Content-Type: application/json

{"node_id":"auth.example.com|0123456789abcdef0123456789abcdef","secret":"opaque-secret"}
```

An incoming `Origin` header is forwarded unchanged to the verifier; if absent, no `Origin` header is added. The authority decides whether that origin is permitted. This also applies to lookup and renewal, including renewal at a non-owner server. `/create` supports CORS from any domain but only constructs credentials; it does not call the verifier.

The JSON body contains `node_id` and `secret`. The node ID is also retained in the query for compatibility. Existing `node_id` query values are replaced; other parameters are preserved. The router controls the `action` query parameter, adding `action=renew` for renewal and removing it for ordinary authentication.

Approval requires HTTP 200 and JSON:

```json
{"valid":"true","rp5":10,"maxc":5}
```

Boolean `true` is also accepted. `maxc` is an optional positive integer, defaulting to 5, that limits tracked `/connect` sockets per node. A new connection evicts and closes the oldest tracked socket when the limit is reached. A lowered limit evicts as many oldest sockets as needed before tracking the new one. Eviction is an intentional close and does not replay recent packets. `rp5` must be a positive integer and becomes the connection's allowance per five-second bucket. Plain `ok`, false or missing validity, missing or invalid rates, redirects, certificate failures, and timeouts deny authentication. Requests have a five-second timeout and responses are limited to 8 KiB.

The verifier also supports optional `x_src_id` and `expires` (Unix seconds) from the authority. Without them, source identity defaults to `node_id`, and validity is decided by the authority at each authentication. Renewal can return `secret_string` to rotate the token; otherwise it approves the existing token. The router cannot independently mint a new secret that an external authority will recognize.

## Server registration

`SERVER_SECRET` and `cluster_secret` have been removed. Each process generates a local random secret and uses:

```text
node_id = advertised-host[:port]|ring-position
token   = /server|local-random-secret
```

The joining server calls `POST https://<primary>/server` with JSON `server_domain`, `id`, and `secret`. The primary must verify both the permitted domain relationship and the credential by POSTing JSON containing `node_id` and `secret` to the joining server's HTTPS `/server?node_id=...`. A supplied domain name alone does not prove control. The joining server's listener must be available before registration.

The local `/server` verifier approves only its own node ID and random secret, returning the required JSON. Successful registration distributes the server credential in the ring snapshot over HTTPS. Peer upgrades must match a registered ring identity and pass HTTPS verification. Snapshots must authenticate the configured primary; certificate reload must authenticate the local server credential.

An authenticated node is a server when its authority is the current server hostname, its permitted parent, or a descendant of that parent. For `sub1.x.y.com`, that parent is `x.y.com`. The public suffix list prevents widening trust to `com`, `co.uk`, or shared hosting suffixes such as `github.io`. Hostnames are compared on label boundaries. Ports do not change domain ownership. IP addresses and `localhost` use exact hostname matching.

Renewal authenticates directly through the authority with `action=renew` on whichever server receives the request, then disconnects that node's local sockets. It does not depend on ring ownership or proxy to another router; sockets on other servers remain connected. Rate limits and identity fields come from the authority response. Existing signed tokens require migration to the new node ID and token pair.

## Local HTTPS tests

Automated tests use Go's `httptest.NewTLSServer` and its certificate-trusting client. Certificate verification stays enabled; an ordinary client without the test trust rejects the same server. This runs without installing a CA or needing mkcert in CI. See [Go's httptest documentation](https://pkg.go.dev/net/http/httptest#Server.Client).

The current tests cover JSON POST bodies, query preservation, repeated `node_id` replacement, mandatory JSON validity, authority-provided `rp5`, renewal responses, rejected redirects and certificates, malformed credentials, sibling domains, nested parent domains, unrelated domains, public suffixes, and IP addresses.

The routing fixtures use a local test certificate for `*.router.test` and `auth.client.test`. HTTPS listeners run on loopback while the client validates each advertised hostname. Tests cover registration proof, forged registration denial, primary-only snapshots, authenticated WSS forwarding, renewal disconnection, and routing behavior. The five-server/1,000-node stress test uses the same verified TLS fixtures; its authority returns `rp5: 1000`.

For manual testing, [mkcert](https://github.com/FiloSottile/mkcert) can create locally trusted certificates:

```sh
mkcert -install
mkdir -p /tmp/wsrouter-certs
mkcert -cert-file /tmp/wsrouter-certs/server.pem \
  -key-file /tmp/wsrouter-certs/server.key \
  localhost 127.0.0.1 ::1 sub1.router.test sub2.router.test auth.client.test
```

Map the three `.test` hostnames to `127.0.0.1` in local DNS or `/etc/hosts`, and use separate HTTPS ports. Give each router `--ssl_certs=/tmp/wsrouter-certs` and the corresponding advertised HTTPS URL. The auth fixture must use `auth.client.test` so it remains outside the trusted server parent domain. Containerized processes need the mkcert root certificate in their own trust store; the root is `rootCA.pem` in the directory reported by `mkcert -CAROOT`. No insecure TLS option is needed.
