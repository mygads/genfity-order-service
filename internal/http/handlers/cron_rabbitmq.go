//go:build cron

package handlers

import (
	"net/http"
	"strconv"
	"time"

	"genfity-order-services/internal/queue"
	"genfity-order-services/pkg/response"
)

func (h *Handler) CronEventsToNotificationJobs(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now().UTC()
	max := 50
	if v := r.URL.Query().Get("max"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			if n < 1 {
				n = 1
			}
			if n > 250 {
				n = 250
			}
			max = n
		}
	}

	if h.Queue == nil {
		response.JSON(w, http.StatusOK, map[string]any{
			"success":   true,
			"disabled":  true,
			"processed": 0,
			"errors":    []string{},
			"startedAt": startedAt,
			"endedAt":   time.Now().UTC(),
		})
		return
	}

	processed := 0
	errors := make([]string, 0)

	for i := 0; i < max; i++ {
		msg, ok, err := h.Queue.Get(queue.EventsQueue)
		if err != nil {
			errors = append(errors, err.Error())
			break
		}
		if !ok {
			break
		}

		processed++
		if err := queue.ProcessEventToJobs(r.Context(), h.DB, h.Queue, msg.Body); err != nil {
			errors = append(errors, err.Error())
			_ = msg.Nack(false, true)
			continue
		}

		_ = msg.Ack(false)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":   len(errors) == 0,
		"disabled":  false,
		"processed": processed,
		"errors":    errors,
		"startedAt": startedAt,
		"endedAt":   time.Now().UTC(),
	})
}
