package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/baracaio/rinha-backend-2025/internal/handlers"
	"github.com/baracaio/rinha-backend-2025/internal/processor"
	"github.com/baracaio/rinha-backend-2025/internal/storage"
)

// Config holds application configuration
type Config struct {
	// Server configuration
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// Payment processor URLs
	DefaultProcessorURL  string
	FallbackProcessorURL string

	// Worker configuration
	WorkerCount int

	// Database configuration
	DatabasePath string

	// Health check interval
	HealthCheckInterval time.Duration

	// Queue configuration
	QueueSize      int
	RetryQueueSize int
	MaxRetries     int
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() *Config {
	return &Config{
		// Server defaults
		Port:         getEnv("PORT", "8080"),
		ReadTimeout:  getDurationEnv("READ_TIMEOUT", "10s"),
		WriteTimeout: getDurationEnv("WRITE_TIMEOUT", "15s"),
		IdleTimeout:  getDurationEnv("IDLE_TIMEOUT", "120s"),

		// Payment processor URLs
		DefaultProcessorURL:  getEnv("PAYMENT_PROCESSOR_DEFAULT", "http://payment-processor-default:8080"),
		FallbackProcessorURL: getEnv("PAYMENT_PROCESSOR_FALLBACK", "http://payment-processor-fallback:8080"),

		// Worker configuration
		WorkerCount: getIntEnv("WORKER_COUNT", getOptimalWorkerCount()),

		// Database configuration
		DatabasePath: getEnv("DATABASE_PATH", "./data/payments.db"),

		// Health check configuration
		HealthCheckInterval: getDurationEnv("HEALTH_CHECK_INTERVAL", "5s"),

		// Queue configuration
		QueueSize:      getIntEnv("QUEUE_SIZE", 10000),
		RetryQueueSize: getIntEnv("RETRY_QUEUE_SIZE", 1000),
		MaxRetries:     getIntEnv("MAX_RETRIES", 5),
	}
}

// Application holds all application components
type Application struct {
	config *Config
	server *http.Server

	// Core components
	storage        *storage.SQLiteStorage
	defaultClient  *processor.Client
	fallbackClient *processor.Client
	healthMonitor  *processor.HealthMonitor
	smartRouter    *processor.SmartRouter
	paymentQueue   *processor.PaymentQueue

	// Shutdown
	shutdownChan chan os.Signal
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewApplication creates and initializes a new application instance
func NewApplication(config *Config) (*Application, error) {
	ctx, cancel := context.WithCancel(context.Background())

	app := &Application{
		config:       config,
		shutdownChan: make(chan os.Signal, 1),
		ctx:          ctx,
		cancel:       cancel,
	}

	if err := app.initialize(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize application: %w", err)
	}

	return app, nil
}

// initialize sets up all application components
func (app *Application) initialize() error {
	log.Println("Initializing application...")

	// 1. Initialize storage
	if err := app.initializeStorage(); err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}

	// 2. Initialize payment processor clients
	if err := app.initializeClients(); err != nil {
		return fmt.Errorf("failed to initialize clients: %w", err)
	}

	// 3. Initialize health monitor
	if err := app.initializeHealthMonitor(); err != nil {
		return fmt.Errorf("failed to initialize health monitor: %w", err)
	}

	// 4. Initialize smart router
	if err := app.initializeSmartRouter(); err != nil {
		return fmt.Errorf("failed to initialize smart router: %w", err)
	}

	// 5. Initialize payment queue
	if err := app.initializePaymentQueue(); err != nil {
		return fmt.Errorf("failed to initialize payment queue: %w", err)
	}

	// 6. Initialize HTTP server
	if err := app.initializeServer(); err != nil {
		return fmt.Errorf("failed to initialize server: %w", err)
	}

	log.Println("Application initialized successfully")
	return nil
}

// initializeStorage sets up the SQLite database
func (app *Application) initializeStorage() error {
	log.Printf("Initializing storage at %s", app.config.DatabasePath)

	// Ensure database directory exists
	dbDir := filepath.Dir(app.config.DatabasePath)
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Initialize SQLite storage
	storage, err := storage.NewSQLiteStorage(app.config.DatabasePath)
	if err != nil {
		return fmt.Errorf("failed to create SQLite storage: %w", err)
	}

	app.storage = storage
	log.Println("Storage initialized successfully")
	return nil
}

// initializeClients sets up payment processor clients
func (app *Application) initializeClients() error {
	log.Printf("Initializing payment processor clients")
	log.Printf("Default processor: %s", app.config.DefaultProcessorURL)
	log.Printf("Fallback processor: %s", app.config.FallbackProcessorURL)

	// Create payment processor clients
	app.defaultClient = processor.NewClient(app.config.DefaultProcessorURL)
	app.fallbackClient = processor.NewClient(app.config.FallbackProcessorURL)

	log.Println("Payment processor clients initialized")
	return nil
}

// initializeHealthMonitor sets up the health monitoring system
func (app *Application) initializeHealthMonitor() error {
	log.Println("Initializing health monitor")

	app.healthMonitor = processor.NewHealthMonitor(app.defaultClient, app.fallbackClient)

	log.Printf("Health monitor initialized with %v interval", app.config.HealthCheckInterval)
	return nil
}

// initializeSmartRouter sets up the intelligent payment router
func (app *Application) initializeSmartRouter() error {
	log.Println("Initializing smart router")

	app.smartRouter = processor.NewSmartRouter(
		app.defaultClient,
		app.fallbackClient,
		app.healthMonitor,
	)

	log.Println("Smart router initialized")
	return nil
}

// initializePaymentQueue sets up the async payment processing queue
func (app *Application) initializePaymentQueue() error {
	log.Printf("Initializing payment queue with %d workers", app.config.WorkerCount)

	app.paymentQueue = processor.NewPaymentQueue(
		app.smartRouter,
		app.storage,
		app.config.WorkerCount,
	)

	log.Printf("Payment queue initialized (queue size: %d, retry queue: %d)",
		app.config.QueueSize, app.config.RetryQueueSize)
	return nil
}

// initializeServer sets up the HTTP server with routes and middleware
func (app *Application) initializeServer() error {
	log.Printf("Initializing HTTP server on port %s", app.config.Port)

	// Setup routes
	mux := handlers.SetupRoutes(app.paymentQueue, app.storage)

	// Add middleware stack
	handler := handlers.RecoveryMiddleware(
		handlers.LoggingMiddleware(mux),
	)

	// Create HTTP server with timeouts
	app.server = &http.Server{
		Addr:         ":" + app.config.Port,
		Handler:      handler,
		ReadTimeout:  app.config.ReadTimeout,
		WriteTimeout: app.config.WriteTimeout,
		IdleTimeout:  app.config.IdleTimeout,
	}

	log.Printf("HTTP server configured (timeouts: read=%v, write=%v, idle=%v)",
		app.config.ReadTimeout, app.config.WriteTimeout, app.config.IdleTimeout)
	return nil
}

// Start starts all application components
func (app *Application) Start() error {
	log.Println("Starting application...")

	// Start health monitor
	app.healthMonitor.Start()

	// Start payment queue
	app.paymentQueue.Start()

	// Setup graceful shutdown
	signal.Notify(app.shutdownChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("Server starting on http://localhost:%s", app.config.Port)
		log.Printf("Health endpoint: http://localhost:%s/health", app.config.Port)

		if err := app.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v", err)
			app.shutdownChan <- syscall.SIGTERM
		}
	}()

	// Log startup information
	app.logStartupInfo()

	log.Println("Application started successfully")
	return nil
}

// Wait waits for shutdown signal and handles graceful shutdown
func (app *Application) Wait() {
	// Wait for shutdown signal
	sig := <-app.shutdownChan
	log.Printf("Received signal: %v", sig)

	app.Shutdown()
}

// Shutdown gracefully shuts down the application
func (app *Application) Shutdown() {
	log.Println("Shutting down application...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop accepting new requests
	if err := app.server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	// Stop payment queue (finish processing current payments)
	if app.paymentQueue != nil {
		app.paymentQueue.Stop()
	}

	// Stop health monitor
	if app.healthMonitor != nil {
		app.healthMonitor.Stop()
	}

	// Close storage
	if app.storage != nil {
		if err := app.storage.Close(); err != nil {
			log.Printf("Storage close error: %v", err)
		}
	}

	// Close processor clients
	if app.defaultClient != nil {
		app.defaultClient.Close()
	}
	if app.fallbackClient != nil {
		app.fallbackClient.Close()
	}

	// Cancel application context
	app.cancel()

	log.Println("Application shutdown completed")
}

// logStartupInfo logs important startup information
func (app *Application) logStartupInfo() {
	log.Println("=== Payment Gateway Started ===")
	log.Printf("Version: %s", getVersion())
	log.Printf("Go Version: %s", runtime.Version())
	log.Printf("GOMAXPROCS: %d", runtime.GOMAXPROCS(0))
	log.Printf("Port: %s", app.config.Port)
	log.Printf("Workers: %d", app.config.WorkerCount)
	log.Printf("Database: %s", app.config.DatabasePath)
	log.Printf("Default Processor: %s", app.config.DefaultProcessorURL)
	log.Printf("Fallback Processor: %s", app.config.FallbackProcessorURL)
	log.Println("================================")
}

// Helper functions

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue string) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	if duration, err := time.ParseDuration(defaultValue); err == nil {
		return duration
	}
	return 30 * time.Second // Fallback
}

func getOptimalWorkerCount() int {
	// Use number of CPU cores, but cap at 10 for resource constraints
	cpus := runtime.NumCPU()
	if cpus > 10 {
		return 10
	}
	if cpus < 2 {
		return 2
	}
	return cpus
}

func getVersion() string {
	return getEnv("APP_VERSION", "1.0.0")
}

// main is the application entry point
func main() {
	// Configure logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[payment-gateway] ")

	log.Println("Payment Gateway starting...")

	// Load configuration
	config := LoadConfig()

	// Create application
	app, err := NewApplication(config)
	if err != nil {
		log.Fatalf("Failed to create application: %v", err)
	}

	// Start application
	if err := app.Start(); err != nil {
		log.Fatalf("Failed to start application: %v", err)
	}

	// Wait for shutdown
	app.Wait()

	log.Println("Payment Gateway stopped")
	os.Exit(0)
}

// Health check function for container orchestration
func init() {
	// Set reasonable defaults for production
	if os.Getenv("GOMAXPROCS") == "" {
		// Let the container limit CPU usage
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	// Set GC target percentage for memory efficiency
	if os.Getenv("GOGC") == "" {
		os.Setenv("GOGC", "100")
	}
}
