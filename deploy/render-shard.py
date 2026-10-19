"""Render one independently addressable ring member; apply with kubectl -n wsrouter."""
import argparse
import json

p = argparse.ArgumentParser()
p.add_argument('--name', required=True, help='Kubernetes name, e.g. wsrouter-0')
p.add_argument('--domain', required=True)
p.add_argument('--ring-id', type=int, required=True)
p.add_argument('--ip', required=True)
p.add_argument('--image', required=True)
p.add_argument('--clusteraddr', default='')
p.add_argument('--certbot-image', default='certbot/certbot@sha256:f70ad0adbb7e117f0fe42a63c553f28ea451edabc0148757b6efcd9735acaa20')
a = p.parse_args()
if not 0 <= a.ring_id <= 9223372036854775807:
    p.error('ring-id must be between 0 and MaxInt64')
labels = {'app.kubernetes.io/name': 'wsrouter', 'wsrouter-shard': a.name}
meta = {'name': a.name, 'namespace': 'wsrouter', 'labels': labels}
args = ['/usr/local/bin/wsrouter', '--addr=https://' + a.domain, '--id=' + str(a.ring_id), '--ssl_certs=/tls/current']
if a.clusteraddr:
    args.append('--clusteraddr=' + a.clusteraddr)
service = {'apiVersion': 'v1', 'kind': 'Service', 'metadata': dict(meta, annotations={'cloud.google.com/l4-rbs': 'enabled'}), 'spec': {
    'type': 'LoadBalancer', 'loadBalancerIP': a.ip, 'externalTrafficPolicy': 'Cluster',
    # HTTP-01 must reach Certbot even before the router has its first certificate.
    'publishNotReadyAddresses': True, 'selector': labels,
    'ports': [{'name': 'https', 'port': 443, 'targetPort': 443}, {'name': 'http-acme', 'port': 80, 'targetPort': 8080}]
}}
internal = {'apiVersion': 'v1', 'kind': 'Service', 'metadata': dict(meta, name=a.name+'-internal'), 'spec': {
    'clusterIP': 'None', 'selector': labels, 'ports': [{'name':'https','port':443,'targetPort':443}]
}}
router = {'name': 'router', 'image': a.image, 'imagePullPolicy': 'IfNotPresent',
    'command': ['/bin/sh', '-ec'], 'args': ['until [ -r /tls/current/server.pem ] && [ -r /tls/current/server.key ]; do sleep 5; done; exec "$@"', 'router'] + args,
    'ports': [{'name': 'https', 'containerPort': 443}],
    'resources': {'requests': {'cpu': '100m', 'memory': '128Mi'}, 'limits': {'cpu': '2', 'memory': '1Gi'}},
    'securityContext': {'runAsUser': 65532, 'runAsNonRoot': True, 'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL'], 'add': ['NET_BIND_SERVICE']}},
    'readinessProbe': {'httpGet': {'scheme': 'HTTPS', 'path': '/health', 'port': 443}, 'periodSeconds': 5},
    'volumeMounts': [{'name': 'tls', 'mountPath': '/tls', 'readOnly': True}]
}
certbot = {'name': 'certbot', 'image': a.certbot_image,
    'command': ['python', '-u', '/scripts/certbot-runner.py'],
    'env': [{'name': 'DOMAIN', 'value': a.domain}, {'name': 'EXPECTED_IP', 'value': a.ip}],
    'ports': [{'name': 'http-acme', 'containerPort': 8080}],
    'resources': {'requests': {'cpu': '25m', 'memory': '64Mi'}, 'limits': {'cpu': '500m', 'memory': '256Mi'}},
    'securityContext': {'runAsUser': 0, 'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
    'readinessProbe': {'httpGet': {'path': '/health', 'port': 8080}, 'periodSeconds': 5},
    'livenessProbe': {'httpGet': {'path': '/health', 'port': 8080}, 'initialDelaySeconds': 15, 'periodSeconds': 30},
    'volumeMounts': [{'name': 'letsencrypt', 'mountPath': '/etc/letsencrypt'}, {'name': 'acme', 'mountPath': '/var/acme'}, {'name': 'tls', 'mountPath': '/tls'}, {'name': 'scripts', 'mountPath': '/scripts', 'readOnly': True}]
}
stateful = {'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'metadata': meta, 'spec': {
    'replicas': 1, 'serviceName': a.name+'-internal', 'selector': {'matchLabels': labels},
    'template': {'metadata': {'labels': labels}, 'spec': {
        'automountServiceAccountToken': False, 'terminationGracePeriodSeconds': 30,
        'nodeSelector': {'cloud.google.com/gke-nodepool': 'servers-e2-standard'},
        'securityContext': {'runAsUser': 65532, 'runAsGroup': 65532, 'fsGroup': 65532, 'fsGroupChangePolicy': 'OnRootMismatch', 'seccompProfile': {'type': 'RuntimeDefault'}},
        'containers': [router, certbot],
        'volumes': [{'name': 'tls', 'emptyDir': {}}, {'name': 'acme', 'emptyDir': {}}, {'name': 'scripts', 'configMap': {'name': 'wsrouter-certbot'}}]
    }},
    'volumeClaimTemplates': [{'metadata': {'name': 'letsencrypt'}, 'spec': {'accessModes': ['ReadWriteOnce'], 'storageClassName': 'standard-rwo', 'resources': {'requests': {'storage': '1Gi'}}}}]
}}
print(json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': [service, internal, stateful]}, indent=2))
