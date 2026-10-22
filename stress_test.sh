#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
# Same toolchain as production, fixed CPU budget, and a fresh test process per run.
builder=$(awk '$1 == "FROM" { print $2; exit }' Dockerfile)
docker run --rm --cpus="${CPUS:-4}" --memory="${MEMORY:-6g}" \
    --mount "type=bind,source=$PWD,target=/src,readonly" \
    --mount type=volume,source=wsrouter-perf-go-cache,target=/root/.cache/go-build \
    --mount type=volume,source=wsrouter-perf-go-mod,target=/go/pkg/mod \
    --workdir /src -e "GOMAXPROCS=${CPUS:-4}" -e "RUNS=${RUNS:-3}" \
    -e WSROUTER_STRESS=1 -e "WSROUTER_STRESS_NODES=${WSROUTER_STRESS_NODES:-1000}" \
    -e "WSROUTER_STEADY=${WSROUTER_STEADY:-0}" "$builder" sh -ec '
    go version
    i=0
    while [ "$i" -lt "$RUNS" ]; do
        go test -run "^TestStressFiveServers$" -count=1 -v .
        i=$((i+1))
    done
    if [ "$WSROUTER_STEADY" = 1 ]; then
        go test -run "^TestStressSteadyRouting$" -count=1 -v .
    fi
    '
