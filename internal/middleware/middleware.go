// Package middleware provides Gin middleware for logging, metrics, rate limiting and auth.
package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests processed.",
	}, []string{"method", "path", "status"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})
)

// Logger returns a Gin middleware that logs each request with zap.
func Logger(log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.FullPath()),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("duration", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
		)
	}
}

// Metrics records Prometheus counters and histograms per request.
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		status := strconv.Itoa(c.Writer.Status())
		httpRequestsTotal.WithLabelValues(c.Request.Method, c.FullPath(), status).Inc()
		httpRequestDuration.WithLabelValues(c.Request.Method, c.FullPath()).Observe(time.Since(start).Seconds())
	}
}

// StoreIDRequired validates that a store_id header or param is present.
// Real-world: replace with JWT validation and extract store context from claims.
func StoreIDRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		storeID := c.GetHeader("X-Store-ID")
		if storeID == "" {
			storeID = c.Param("store_id")
		}
		if storeID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "X-Store-ID header required"})
			return
		}
		c.Set("store_id", storeID)
		c.Next()
	}
}

// Recovery wraps gin.Recovery with structured logging.
func Recovery(log *zap.Logger) gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		log.Error("panic recovered", zap.Any("error", recovered))
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
	})
}
