// Command ingestion runs the Event Ingestion Service.
// It exposes HTTP endpoints to receive tracking events and publishes them to Kafka.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/nidhi24rajput/marketpulse/internal/config"
	"github.com/nidhi24rajput/marketpulse/internal/ingestion"
	mkafka "github.com/nidhi24rajput/marketpulse/internal/kafka"
	"github.com/nidhi24rajput/marketpulse/internal/middleware"
)

func main() {
	// ── Logger ────────────────────────────────────────────────────────────────
	log, _ := zap.NewProduction()
	defer func() { _ = log.Sync() }()

	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config", zap.Error(err))
	}

	// ── Kafka Producer ────────────────────────────────────────────────────────
	producer, err := mkafka.NewProducer(cfg.Kafka.Brokers, log)
	if err != nil {
		log.Fatal("failed to create kafka producer", zap.Error(err))
	}
	defer producer.Close()

	// ── HTTP Server ───────────────────────────────────────────────────────────
	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(middleware.Recovery(log))
	router.Use(middleware.Logger(log))
	router.Use(middleware.Metrics())

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "ingestion"})
	})
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	h := ingestion.NewHandler(producer, cfg.Kafka.EventTopic, log)
	v1 := router.Group("/v1")
	{
		v1.POST("/events", middleware.StoreIDRequired(), h.TrackEvent)
		v1.POST("/events/batch", middleware.StoreIDRequired(), h.TrackBatch)
		v1.POST("/orders", middleware.StoreIDRequired(), h.TrackOrder)
	}

	srv := &http.Server{
		Addr:         ":" + cfg.App.IngestionPort,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ── Graceful Shutdown ─────────────────────────────────────────────────────
	go func() {
		log.Info("ingestion service starting", zap.String("port", cfg.App.IngestionPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down ingestion service")
	ctx, cancel := context.WithTimeout(context.Background(), cfg.App.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("shutdown error", zap.Error(err))
	}
}
