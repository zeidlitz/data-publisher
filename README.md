## data publisher 

data publishing service that acts as a logical middle layer service between a messaging layer (redis) and a storage layer (duckdb)

```
┌─────┐  ┌──────────────┐  ┌──────┐ 
│redis│<-│data-publisher│->│duckdb│ 
└─────┘  └──────────────┘  └──────┘ 
```

this service is not agnostic to the message and storage layer

## configuration

consumes the following environment variables

| Name | Type | Default |
| --------------- | --------------- | --------------- |
| DUCK_DB_CONNECTION | string | "data/sentiment.duckdb"
| REDIS_ADDRESS | string | "localhost:6379"
| REDIS_STREAM | string | "analysis_results"
| REDIS_GROUP | string | "data_publisher"
| REDIS_CONSUMER | string | "publisher"

## usage

the goal is to run this as a service, for example [systemd](https://systemd.io/), see the specific system manager docs on details

running from source should follow ideomatic golang practices

example on fetching the source and ensuring the default data directory exists relative to the root

```bash
git clone git@github.com:zeidlitz/data-publisher.git 
cd data-publisher
mkdir -p data
touch data/sentiment.duckdb
chmod +x data/sentiment.duckdb
```

grab any dependencies
```bash
go mod tidy
```

run from the entrypoint in `cmd/`

```bash
go run cmd/publisher.go
```

or build the binary

```bash
go build cmd/publisher.go
```
