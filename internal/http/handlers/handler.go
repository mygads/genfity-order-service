package handlers

import (
	"genfity-order-services/internal/config"
	"genfity-order-services/internal/queue"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Handler struct {
	DB     *pgxpool.Pool
	Logger *zap.Logger
	Config config.Config
	Queue  *queue.Client
}
