package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"genfity-order-services/internal/config"
	"genfity-order-services/internal/db"
	httpapi "genfity-order-services/internal/http"
	"genfity-order-services/internal/logger"
	"genfity-order-services/internal/queue"
	"genfity-order-services/internal/ws"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	_ = godotenv.Load()

	cfg := config.Load()
	log, err := logger.New(cfg.Env)
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	ctx := context.Background()
	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("database connection failed", zap.Error(err))
	}
	defer pool.Close()

	var queueClient *queue.Client
	if cfg.RabbitMQURL != "" {
		log.Info("rabbitmq enabled", zap.String("eventsQueue", "genfity.notifications"))
		qc, err := queue.New(cfg.RabbitMQURL)
		if err != nil {
			if cfg.Env == "production" {
				log.Fatal("rabbitmq connection failed", zap.Error(err))
			}
			log.Warn("rabbitmq connection failed; continuing without worker", zap.Error(err))
			qc = nil
		}
		if qc != nil {
			if err := qc.EnsureExchange("genfity.events"); err != nil {
				if cfg.Env == "production" {
					log.Fatal("rabbitmq exchange failed", zap.Error(err))
				}
				log.Warn("rabbitmq exchange failed; continuing without worker", zap.Error(err))
				_ = qc.Close()
				qc = nil
			}
		}
		if qc != nil {
			if _, err := qc.EnsureQueue("genfity.notifications"); err != nil {
				if cfg.Env == "production" {
					log.Fatal("rabbitmq queue failed", zap.Error(err))
				}
				log.Warn("rabbitmq queue failed; continuing without worker", zap.Error(err))
				_ = qc.Close()
				qc = nil
			}
		}
		if qc != nil {
			// Use topic wildcard '#' to include multi-segment routing keys like
			// 'order.status.updated'. The '*' wildcard only matches a single segment.
			if err := qc.BindQueue("genfity.notifications", "genfity.events", "order.#"); err != nil {
				if cfg.Env == "production" {
					log.Fatal("rabbitmq bind failed", zap.Error(err))
				}
				log.Warn("rabbitmq bind failed; continuing without worker", zap.Error(err))
				_ = qc.Close()
				qc = nil
			}
		}

		if qc != nil {
			if err := queue.EnsureNotificationJobsTopology(ctx, qc); err != nil {
				if cfg.Env == "production" {
					log.Fatal("rabbitmq notification_jobs topology failed", zap.Error(err))
				}
				log.Warn("rabbitmq notification_jobs topology failed; continuing without worker", zap.Error(err))
				_ = qc.Close()
				qc = nil
			}
		}
		if qc != nil {
			if err := queue.EnsureCompletedEmailTopology(ctx, qc); err != nil {
				if cfg.Env == "production" {
					log.Fatal("rabbitmq completed_email topology failed", zap.Error(err))
				}
				log.Warn("rabbitmq completed_email topology failed; continuing without worker", zap.Error(err))
				_ = qc.Close()
				qc = nil
			}
		}

		queueClient = qc
		if qc != nil {
			defer qc.Close()
		}

		if queueClient != nil {
			if cfg.RabbitMQWorkerMode == "daemon" {
				log.Info("event translator enabled", zap.String("mode", "daemon"))
				go func() {
					err := queueClient.ConsumeWithRetry("genfity.notifications", func(ctx context.Context, body []byte) error {
						// Translate events into notification jobs / completed-email jobs.
						return queue.ProcessEventToJobs(ctx, pool, queueClient, body)
					}, 5, 5*time.Second)
					if err != nil {
						log.Error("consumer stopped", zap.Error(err))
					}
				}()
			} else {
				log.Info("event translator disabled", zap.String("mode", cfg.RabbitMQWorkerMode))
			}
		}
	} else {
		log.Info("order worker disabled (RABBITMQ_URL is empty)")
	}

	wsServer := ws.New(pool, log, cfg)
	apiServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      httpapi.NewRouter(pool, log, cfg, queueClient, wsServer),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("order api ready", zap.String("base", "/api"))
		log.Info("order ws ready", zap.String("base", "/ws"))
		log.Info("order service listening", zap.String("addr", cfg.HTTPAddr))
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("http server failed", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctxShutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := apiServer.Shutdown(ctxShutdown); err != nil {
		log.Error("http server shutdown failed", zap.Error(err))
	}
}
