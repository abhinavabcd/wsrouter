# chatapp.world router deployment

## Current installation

All Kubernetes resources are in namespace `wsrouter` on context
`gke_sukhiba-e4413_europe-central2-c_sukhiba-prod`, project `sukhiba-e4413`.
The cluster is in Poland (`europe-central2`); `nl` in the hostname is only a name.

| Setting | Value |
| --- | --- |
| Public address | `https://0.nl.chatapp.world` / `wss://0.nl.chatapp.world/connect` |
| Static regional IP | `34.118.100.7` (`wsrouter-0-nl`) |
| Ring ID | `0` (leader; no `--clusteraddr`) |
| StatefulSet / pod | `wsrouter-0` / `wsrouter-0-0` |
| Service ClusterIP | `10.112.15.40` |
| Certificate PVC | `letsencrypt-wsrouter-0-0` (1 GiB) |
| Image tag | `europe-central2-docker.pkg.dev/sukhiba-e4413/apps/wsrouter:20261027T135236Z` |
| Go builder | `golang:1.27.3-trixie` |
| Deployed digest | `sha256:568230b61c249a973bd623b3c904aaf788ab46ce0fd5e2c8d8057a10f44b78e0` |

Run the commands below from this directory, using the context above.

## DNS and first certificate

Add this record at the DNS provider for `chatapp.world`:

| Type | Name | Value | TTL |
| --- | --- | --- | --- |
| A | `0.nl` | `34.118.100.7` | 300 |

Use DNS-only mode. Remove any conflicting AAAA record for this hostname.
Port 80 must stay reachable for HTTP-01 renewals; port 443 goes directly to
Go's TLS listener. There is no TLS terminating proxy.

Certbot registers **without an email address**. It waits for the hostname to
resolve to the reserved IP, validates against the staging CA with a dry run,
then requests the production certificate. Until issuance succeeds the router
waits for its certificate and the pod is not fully ready. The Service's
`publishNotReadyAddresses: true` permits the initial HTTP-01 challenge to reach
Certbot during this bootstrap period.

Certbot stores its account and certificates on the PVC. It checks for renewal
at startup and every 12 hours with up to 30 minutes of jitter. A validated
certificate/key pair is published by an atomic symlink replacement in a shared
volume. The router checks that directory every 30 seconds and reloads TLS
without closing existing WebSockets. Failed reloads retain the previous pair.
The router cannot read Certbot's account directory.

```sh
kubectl -n wsrouter get pods,services,pvc
kubectl -n wsrouter logs wsrouter-0-0 -c certbot --tail=100
kubectl -n wsrouter logs wsrouter-0-0 -c router --tail=100
curl --fail https://0.nl.chatapp.world/health
kubectl -n wsrouter exec wsrouter-0-0 -c certbot -- certbot certificates
```

After production issuance, verify renewal without replacing the live certificate:

```sh
kubectl -n wsrouter exec wsrouter-0-0 -c certbot -- certbot renew \
  --cert-name 0.nl.chatapp.world --dry-run --no-random-sleep-on-renew \
  --work-dir /var/acme/work --logs-dir /var/acme/logs
```

If Certbot reports a lock, retry after the scheduled worker finishes. Monitor
certificate expiry and renewal errors; no email notifications are configured.

## Reapply or update

The checked-in `deploy/chatworld-nl-0.yaml` pins the router and Certbot images by digest.
The deployment targets the `servers-e2-standard` node pool.

```sh
kubectl -n wsrouter create configmap wsrouter-certbot \
  --from-file=certbot-runner.py=deploy/certbot-runner.py \
  --dry-run=client -o yaml > deploy/certbot-configmap.yaml
kubectl apply -f deploy/certbot-configmap.yaml -f deploy/chatworld-nl-0.yaml
```

Deploy the current source with the production script:

```sh
./prod_deploy.sh
# Or use an explicit image tag:
./prod_deploy.sh 20261022-router-update
```

The script reads the Go builder image from `Dockerfile`, runs race tests,
`go vet`, and the five-server/1,000-node stress test, builds and pushes the image,
then deploys its immutable digest. It targets the production context explicitly,
waits for readiness and checks public HTTPS health. After success it updates the
local manifest and the image table above. It prints the previous image and a
rollback command before changing production. A failed check stops the script.
The script updates only the router image; apply infrastructure changes separately.

`RUN_STRESS=0 ./prod_deploy.sh` skips the stress test. `KUBE_CONTEXT` overrides the
default production context when needed. Docker registry credentials, kubectl
access, Docker, curl and Python 3 must already be available.

An image update replaces the pod and clients must reconnect. A certificate
renewal alone does not replace the pod. To activate changes to the Python
Certbot worker, update the ConfigMap and restart the StatefulSet:

```sh
kubectl -n wsrouter rollout restart statefulset/wsrouter-0
```

Keep the certificate PVC through updates and restarts.

## Add a shard

Each ring member needs its own unique ring ID, hostname, static IP, Service,
StatefulSet and certificate PVC. **Keep each StatefulSet at one replica**;
scaling it up would duplicate the ring identity. Ring IDs span 0 to MaxInt64.
For two approximately equal shards use `0` and `4611686018427387904`.
Additional members can split those intervals.

```sh
gcloud compute addresses create wsrouter-1-nl \
  --project=sukhiba-e4413 --region=europe-central2 --network-tier=PREMIUM
SHARD_IP=$(gcloud compute addresses describe wsrouter-1-nl \
  --project=sukhiba-e4413 --region=europe-central2 --format='value(address)')
python3 deploy/render-shard.py --name wsrouter-1 \
  --domain 1.nl.chatapp.world --ring-id 4611686018427387904 --ip "$SHARD_IP" \
  --clusteraddr https://0.nl.chatapp.world \
  --image europe-central2-docker.pkg.dev/sukhiba-e4413/apps/wsrouter@sha256:ccafbd5727dd8b18698f5b3cbb298e85e5d4bce4d7e259ed38c0cabe40cced51 \
  > deploy/wsrouter-1.json
kubectl apply -f deploy/wsrouter-1.json
```

Add a DNS-only A record `1.nl` pointing at `SHARD_IP`. Certificate issuance and
cluster registration then run automatically. Sibling hosts share the parent
`nl.chatapp.world`; membership still requires HTTPS verification. Each Service
also has an internal ClusterIP, but advertise the public hostname so clients
and peers can verify TLS. Do not substitute a ClusterIP for the advertised
HTTPS identity. Each shard incurs its own load balancer, disk and compute costs.

Members refresh their ring snapshot every minute. Changed ownership tells
clients to reconnect. The leader is the membership coordinator and is not
highly available; adding router shards does not replicate the coordinator.

## Remove a shard / recovery

Remove a follower from client bootstrap configuration, then scale its StatefulSet
to zero. Clients must reconnect elsewhere. Allow at least three minutes for
membership expiry and the next refresh before deleting its Service. Preserve
the PVC and reserved IP if the shard may return. Removing the leader requires
planning a new coordinator and changing followers' `--clusteraddr` settings.

Pod replacement reconstructs TLS publication from the PVC. Back up that PVC
if retaining the ACME account and certificate history is required. Do not delete
the namespace or PVCs during routine upgrades. Router connection state and
recent replay queues are in memory; pod loss does not preserve those queues.

For protocol and verifier configuration see [README.md](README.md) and
[DOMAIN_AUTH.md](DOMAIN_AUTH.md).
