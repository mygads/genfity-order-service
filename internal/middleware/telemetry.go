package middleware

import (
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

type telemetryRecorder struct {
	response http.ResponseWriter
	status   int
	bytes    int
}

type latencyWindow struct {
	samples []int64
	index   int
	count   int
}

func (w *latencyWindow) add(value int64, max int) {
	if len(w.samples) < max {
		w.samples = append(w.samples, value)
		w.count = len(w.samples)
		return
	}
	w.samples[w.index] = value
	w.index = (w.index + 1) % max
	w.count = max
}

func (w *latencyWindow) snapshot() []int64 {
	if w.count == 0 {
		return nil
	}
	values := make([]int64, 0, w.count)
	if len(w.samples) == w.count {
		values = append(values, w.samples...)
		return values
	}
	values = append(values, w.samples[:w.count]...)
	return values
}

type latencyAggregator struct {
	mu     sync.Mutex
	window int
	routes map[string]*latencyWindow
}

func newLatencyAggregator(window int) *latencyAggregator {
	return &latencyAggregator{window: window, routes: make(map[string]*latencyWindow)}
}

func (a *latencyAggregator) record(key string, value int64) (int64, int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	win, ok := a.routes[key]
	if !ok {
		win = &latencyWindow{}
		a.routes[key] = win
	}
	win.add(value, a.window)

	values := win.snapshot()
	if len(values) == 0 {
		return 0, 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return percentile(values, 0.5), percentile(values, 0.95)
}

func percentile(values []int64, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

var telemetryLatency = newLatencyAggregator(200)

func (r *telemetryRecorder) Header() http.Header {
	return r.response.Header()
}

func (r *telemetryRecorder) WriteHeader(status int) {
	r.status = status
	r.response.WriteHeader(status)
}

func (r *telemetryRecorder) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.response.Write(data)
	r.bytes += n
	return n, err
}

func Telemetry(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			recorder := &telemetryRecorder{response: w}

			next.ServeHTTP(recorder, r)

			status := recorder.status
			if status == 0 {
				status = http.StatusOK
			}

			duration := time.Since(start)
			if logger != nil {
				routePattern := ""
				if rc := chi.RouteContext(r.Context()); rc != nil {
					routePattern = rc.RoutePattern()
				}
				metricKey := r.Method + " " + routePattern
				if routePattern == "" {
					metricKey = r.Method + " " + r.URL.Path
				}
				p50, p95 := telemetryLatency.record(metricKey, duration.Milliseconds())
				requestID := readRequestID(r)
				logger.Info(
					"http_request",
					zap.String("method", r.Method),
					zap.String("path", r.URL.Path),
					zap.String("routePattern", routePattern),
					zap.String("requestId", requestID),
					zap.Int("status", status),
					zap.Int("bytes", recorder.bytes),
					zap.Int64("duration_ms", duration.Milliseconds()),
					zap.Int64("p50_ms", p50),
					zap.Int64("p95_ms", p95),
					zap.Bool("error", status >= 500),
					zap.Bool("clientError", status >= 400 && status < 500),
				)
			}
		})
	}
}

func readRequestID(r *http.Request) string {
	for _, key := range []string{"X-Request-Id", "X-Correlation-Id"} {
		if value := strings.TrimSpace(r.Header.Get(key)); value != "" {
			return value
		}
	}
	return ""
}
