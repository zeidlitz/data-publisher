package config

import (
	"os"
)

type Config struct {
	DuckDbConn    string
	RedisAddr     string
	RedisStream   string
	RedisGroup    string
	RedisConsumer string
}

func New() (Config, error) {
	var config = Config{}

	duckDbConn, found := os.LookupEnv("DUCK_DB_CONNECTION")
	if !found {
		duckDbConn = "data/sentiment.duckdb"
	}

	redisAddr, found := os.LookupEnv("REDIS_ADDRESS")
	if !found {
		redisAddr = "localhost:6379"
	}

	redisStream, found := os.LookupEnv("REDIS_STREAM")
	if !found {
		redisStream = "data_analysis"
	}

	redisGroup, found := os.LookupEnv("REDIS_GROUP")
	if !found {
		redisGroup = "data_analysis"
	}

	redisConsumer, found := os.LookupEnv("REDIS_CONSUMER")
	if !found {
		redisConsumer = "publisher"
	}

	config = Config{
		DuckDbConn:    duckDbConn,
		RedisAddr:     redisAddr,
		RedisStream:   redisStream,
		RedisGroup:    redisGroup,
		RedisConsumer: redisConsumer,
	}

	return config, nil
}
