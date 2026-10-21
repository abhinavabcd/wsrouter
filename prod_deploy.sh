#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} == --help || $# -gt 1 ]]; then
    echo "Usage: $0 [image-tag]"
    echo "Tests, builds, pushes and deploys wsrouter-0 to production."
    echo "Optional environment: KUBE_CONTEXT, RUN_STRESS (default 1)."
    exit 0
fi

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
context=${KUBE_CONTEXT:-gke_sukhiba-e4413_europe-central2-c_sukhiba-prod}
repository=europe-central2-docker.pkg.dev/sukhiba-e4413/apps/wsrouter
tag=${1:-$(date -u +%Y%m%dT%H%M%SZ)}
image="$repository:$tag"
builder=$(awk '$1 == "FROM" { print $2; exit }' Dockerfile)

for command in docker kubectl curl python3; do
    command -v "$command" >/dev/null
done
previous=$(kubectl --context "$context" -n wsrouter get statefulset wsrouter-0 \
    -o 'jsonpath={.spec.template.spec.containers[?(@.name=="router")].image}')

echo "Testing with $builder"
docker run --rm --mount "type=bind,source=$PWD,target=/src,readonly" \
    --workdir /src -e "RUN_STRESS=${RUN_STRESS:-1}" "$builder" sh -ec '
    if ! command -v gcc >/dev/null; then
        apk add --no-cache gcc musl-dev
    fi
    go version
    go test -race ./...
    go vet ./...
    if [ "$RUN_STRESS" = 1 ]; then
        WSROUTER_STRESS=1 go test -run TestStressFiveServers -count=1 -v .
    fi
    '

docker build --pull -t "$image" .
docker run --rm "$image" --help
docker push "$image"
deployed=$(docker image inspect "$image" --format '{{range .RepoDigests}}{{println .}}{{end}}' \
    | awk -v prefix="$repository@sha256:" 'index($0, prefix) == 1 { print; exit }')
[[ -n "$deployed" ]] || { echo "Pushed image digest missing" >&2; exit 1; }

echo "Deploying $deployed"
echo "Previous image: $previous"
echo "Rollback: kubectl --context $context -n wsrouter set image statefulset/wsrouter-0 router=$previous"
kubectl --context "$context" -n wsrouter set image statefulset/wsrouter-0 "router=$deployed"
kubectl --context "$context" -n wsrouter rollout status statefulset/wsrouter-0 --timeout=300s
curl --fail --silent --show-error --retry 5 --retry-connrefused --retry-delay 2 \
    --max-time 15 https://0.nl.chatapp.world/health

# Keep subsequent manifest applies pinned to the successfully deployed image.
python3 - "$deployed" "$image" "$builder" <<'PY'
from pathlib import Path
import re
import sys

digest, image, builder = sys.argv[1:]
manifest = Path('deploy/chatworld-nl-0.yaml')
text, count = re.subn(
    r'(?m)^(\s+image: )europe-central2-docker\.pkg\.dev/sukhiba-e4413/apps/wsrouter[^\s]*$',
    lambda match: match[1] + digest,
    manifest.read_text(),
)
if count != 1:
    raise SystemExit('Deployment succeeded, but the local manifest image could not be updated')
manifest.write_text(text)
guide = Path('chatapp.world.README.md')
text = guide.read_text()
for label, value in [('Image tag', image), ('Go builder', builder), ('Deployed digest', digest.split('@')[1])]:
    text = re.sub(r'(?m)^\| ' + label + r' \|.*$', lambda _: f'| {label} | `{value}` |', text)
guide.write_text(text)
PY
printf '\nDeployed %s\n' "$deployed"
