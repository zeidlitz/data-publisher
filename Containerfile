FROM golang:1.24 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV CGO_ENABLED=1
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-linkmode external -extldflags '-static'" -o publisher ./cmd/publisher.go

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/local/bin/
COPY --from=builder /app/publisher .
ENTRYPOINT ["./publisher"]
