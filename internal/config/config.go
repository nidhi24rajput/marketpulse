package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	App      AppConfig
	Mongo    MongoConfig
	Kafka    KafkaConfig
	Redis    RedisConfig
	Metrics  MetricsConfig
}

type AppConfig struct {
	Env            string
	IngestionPort  string
	AnalyticsPort  string
	ShutdownTimeout time.Duration
}

type MongoConfig struct {
	URI      string
	Database string
}

type KafkaConfig struct {
	Brokers       string
	EventTopic    string
	ConsumerGroup string
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	TTL      time.Duration
}

type MetricsConfig struct {
	Enabled bool
}

// Load reads config from environment, with .env file support for local dev.
func Load() (*Config, error) {
	// Best-effort: load .env if present (ignored in production)
	_ = godotenv.Load()

	redisTTL, _ := strconv.Atoi(getEnv("REDIS_CACHE_TTL_SECONDS", "300"))
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))
	metricsEnabled, _ := strconv.ParseBool(getEnv("METRICS_ENABLED", "true"))

	return &Config{
		App: AppConfig{
			Env:             getEnv("APP_ENV", "development"),
			IngestionPort:   getEnv("INGESTION_PORT", "8080"),
			AnalyticsPort:   getEnv("ANALYTICS_PORT", "8081"),
			ShutdownTimeout: 15 * time.Second,
		},
		Mongo: MongoConfig{
			URI:      getEnv("MONGO_URI", "mongodb://localhost:27017"),
			Database: getEnv("MONGO_DATABASE", "marketpulse"),
		},
		Kafka: KafkaConfig{
			Brokers:       getEnv("KAFKA_BROKERS", "localhost:9092"),
			EventTopic:    getEnv("KAFKA_EVENT_TOPIC", "ecommerce.events"),
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "marketpulse-processor"),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       redisDB,
			TTL:      time.Duration(redisTTL) * time.Second,
		},
		Metrics: MetricsConfig{
			Enabled: metricsEnabled,
		},
	}, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
