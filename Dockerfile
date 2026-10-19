FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY main.go ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /wsrouter .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /wsrouter /usr/local/bin/wsrouter
USER 65532:65532
# --addr is the public base URL (also what /locate hands to clients), the port is taken from it.
# Mount the certificate directory and pass --ssl_certs; authentication uses HTTPS verifiers.
ENTRYPOINT ["wsrouter"]
