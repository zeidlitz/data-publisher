# build stage
FROM golang:1.24 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++
WORKDIR /app
ENV CGO_ENABLED=1
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o publisher ./cmd/publisher.go

# run stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates
RUN apk add --no-cache libstdc++
WORKDIR /root/
COPY --from=builder /app/publisher .
CMD ["./publisher"]
