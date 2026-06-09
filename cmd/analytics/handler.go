package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/nidhi24rajput/marketpulse/internal/analytics"
)

// Handler holds the HTTP handlers for the analytics API.
type Handler struct {
	svc *analytics.Service
	log *zap.Logger
}

func NewHandler(svc *analytics.Service, log *zap.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

// Revenue godoc
// @Summary     Revenue overview
// @Description Returns daily revenue aggregates for a date range
// @Tags        analytics
// @Produce     json
// @Param       X-Store-ID  header   string true "Store identifier"
// @Param       from        query    string false "Start date (YYYY-MM-DD), default 30 days ago"
// @Param       to          query    string false "End date (YYYY-MM-DD), default today"
// @Success     200 {array} domain.DailyAggregate
// @Router      /v1/analytics/revenue [get]
func (h *Handler) Revenue(c *gin.Context) {
	storeID, _ := c.Get("store_id")
	from, to := parseDateRange(c)

	data, err := h.svc.RevenueOverview(c.Request.Context(), storeID.(string), from, to)
	if err != nil {
		h.log.Error("revenue query failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch revenue data"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": data, "from": from, "to": to})
}

// Funnel godoc
// @Summary     Conversion funnel
// @Description Returns funnel conversion rates from page view to order placed
// @Tags        analytics
// @Produce     json
// @Param       X-Store-ID  header   string true "Store identifier"
// @Param       from        query    string false "Start date"
// @Param       to          query    string false "End date"
// @Success     200 {object} domain.Funnel
// @Router      /v1/analytics/funnel [get]
func (h *Handler) Funnel(c *gin.Context) {
	storeID, _ := c.Get("store_id")
	from, to := parseDateRange(c)

	funnel, err := h.svc.ConversionFunnel(c.Request.Context(), storeID.(string), from, to)
	if err != nil {
		h.log.Error("funnel query failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to compute funnel"})
		return
	}
	c.JSON(http.StatusOK, funnel)
}

// TopProducts godoc
// @Summary     Top products
// @Description Returns top N products ranked by revenue
// @Tags        analytics
// @Produce     json
// @Param       X-Store-ID  header   string true  "Store identifier"
// @Param       limit       query    int    false  "Number of products (default 10, max 50)"
// @Success     200 {array} domain.TopProduct
// @Router      /v1/analytics/top-products [get]
func (h *Handler) TopProducts(c *gin.Context) {
	storeID, _ := c.Get("store_id")
	from, to := parseDateRange(c)
	limit := parseLimit(c, 10, 50)

	products, err := h.svc.TopProducts(c.Request.Context(), storeID.(string), from, to, limit)
	if err != nil {
		h.log.Error("top products query failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch top products"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": products, "from": from, "to": to})
}

// Realtime godoc
// @Summary     Realtime stats
// @Description Returns live activity stats from the last 5 minutes (sourced from Redis)
// @Tags        analytics
// @Produce     json
// @Param       X-Store-ID  header string true "Store identifier"
// @Success     200 {object} domain.RealtimeStats
// @Router      /v1/analytics/realtime [get]
func (h *Handler) Realtime(c *gin.Context) {
	storeID, _ := c.Get("store_id")

	stats, err := h.svc.RealtimeStats(c.Request.Context(), storeID.(string))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch realtime stats"})
		return
	}
	c.JSON(http.StatusOK, stats)
}

// TopSearches returns the most popular search queries.
func (h *Handler) TopSearches(c *gin.Context) {
	storeID, _ := c.Get("store_id")
	from, to := parseDateRange(c)
	limit := parseLimit(c, 20, 100)

	terms, err := h.svc.TopSearchTerms(c.Request.Context(), storeID.(string), from, to, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch search terms"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": terms})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func parseDateRange(c *gin.Context) (time.Time, time.Time) {
	now := time.Now().UTC()
	to := now
	from := now.AddDate(0, 0, -30)

	if f := c.Query("from"); f != "" {
		if t, err := time.Parse("2006-01-02", f); err == nil {
			from = t
		}
	}
	if t := c.Query("to"); t != "" {
		if parsed, err := time.Parse("2006-01-02", t); err == nil {
			to = parsed.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
		}
	}
	return from, to
}

func parseLimit(c *gin.Context, def, max int) int {
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > max {
				return max
			}
			return n
		}
	}
	return def
}
