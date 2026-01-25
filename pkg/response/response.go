package response

import (
	"encoding/json"
	"net/http"
)

func JSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func Success(w http.ResponseWriter, data any) {
	JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    data,
	})
}

func Error(w http.ResponseWriter, status int, code string, message string) {
	JSON(w, status, map[string]any{
		"success": false,
		"error":   code,
		"message": message,
	})
}
