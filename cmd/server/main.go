package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/9ifrashaikh/distributed-system/internal/api"
	"github.com/9ifrashaikh/distributed-system/internal/storage"
)

func main() {
	var (
		port      = flag.String("port", "8080", "Server port")
		storePath = flag.String("storage", "./data", "Storage directory")
	)
	flag.Parse()

	// Initialize storage
	store := storage.NewFileStore(*storePath)

	// Initialize API server
	apiServer := api.NewAPIServer(store)

	// Setup HTTP server
	server := &http.Server{
		Addr:    ":" + *port,
		Handler: apiServer,
	}

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutting down server...")
		server.Close()
	}()

	log.Printf("Starting storage server on port %s", *port)
	log.Printf("Storage directory: %s", *storePath)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed to start: %v", err)
	}
}
