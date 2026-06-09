package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/nidhi24rajput/marketpulse/internal/middleware"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestStoreIDRequired_MissingHeader(t *testing.T) {
	router := gin.New()
	router.GET("/test", middleware.StoreIDRequired(), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestStoreIDRequired_WithHeader(t *testing.T) {
	var capturedStoreID string

	router := gin.New()
	router.GET("/test", middleware.StoreIDRequired(), func(c *gin.Context) {
		capturedStoreID, _ = c.Get("store_id")
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Store-ID", "my-store")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "my-store", capturedStoreID)
}

func TestStoreIDRequired_WithHeader_EmptyValue(t *testing.T) {
	router := gin.New()
	router.GET("/test", middleware.StoreIDRequired(), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Store-ID", "") // empty — should fail
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRecovery_PanicReturns500(t *testing.T) {
	log := zap.NewNop()
	router := gin.New()
	router.Use(middleware.Recovery(log))
	router.GET("/panic", func(c *gin.Context) {
		panic("intentional test panic")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
