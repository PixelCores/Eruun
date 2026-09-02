# syntax=docker/dockerfile:1.7
FROM golang:1.25-alpine AS builder
WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /workspace/eruun-server ./cmd

FROM alpine:3.20
RUN addgroup -S eruun && adduser -S -G eruun eruun
RUN mkdir -p /home/eruun/logs && chown -R eruun:eruun /home/eruun
USER eruun
WORKDIR /home/eruun

COPY --from=builder /workspace/eruun-server /usr/local/bin/eruun-server

EXPOSE 8000
ENTRYPOINT ["eruun-server"]
