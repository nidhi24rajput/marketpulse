// Command analytics runs the Analytics API service.
// It exposes REST endpoints for revenue, funnel, top products and realtime data.
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

	"github.com/nidhi24rajput/marketpulse/internal/analytics"
	"github.com/nidhi24rajput/marketpulse/internal/config"
	"github.com/nidhi24rajput/marketpulse/internal/middleware"
	"github.com/nidhi24rajput/marketpulse/internal/mongodb"
	mredis "github.com/nidhi24rajput/marketpulse/internal/redis"
)

func main() {
	log, _ := zap.NewProduction()
	defer func() { _ = log.Sync() }()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal("config load failed", zap.Error(err))
	}

	// ── MongoDB ───────────────────────────────────────────────────────────────
	mongo, err := mongodb.New(cfg.Mongo.URI, cfg.Mongo.Database)
	if err != nil {
		log.Fatal("mongodb connect failed", zap.Error(err))
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mongo.Disconnect(ctx); err != nil {
			log.Warn("mongo disconnect error", zap.Error(err))
		}
	}()

	// ── Redis ─────────────────────────────────────────────────────────────────
	redisClient, err := mredis.New(cfg.Redis)
	if err != nil {
		log.Fatal("redis connect failed", zap.Error(err))
	}
	defer func() { _ = redisClient.Close() }()

	// ── Service ───────────────────────────────────────────────────────────────
	svc := analytics.NewService(
		mongodb.NewEventRepo(mongo),
		mongodb.NewOrderRepo(mongo),
		mongodb.NewAggregateRepo(mongo),
		redisClient,
		log,
	)

	// ── HTTP ──────────────────────────────────────────────────────────────────
	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(middleware.Recovery(log))
	router.Use(middleware.Logger(log))
	router.Use(middleware.Metrics())

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "analytics"})
	})
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	h := NewHandler(svc, log)
	v1 := router.Group("/v1/analytics")
	v1.Use(middleware.StoreIDRequired())
	{
		v1.GET("/revenue", h.Revenue)
		v1.GET("/funnel", h.Funnel)
		v1.GET("/top-products", h.TopProducts)
		v1.GET("/realtime", h.Realtime)
		v1.GET("/searches", h.TopSearches)
	}

	srv := &http.Server{
		Addr:         ":" + cfg.App.AnalyticsPort,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("analytics service starting", zap.String("port", cfg.App.AnalyticsPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down analytics service")
	ctx, cancel := context.WithTimeout(context.Background(), cfg.App.ShutdownTimeout)
	defer cancel()
	srv.Shutdown(ctx) //nolint:errcheck
}
