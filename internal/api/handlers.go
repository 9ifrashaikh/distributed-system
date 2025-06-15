package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/9ifrashaikh/distributed-system/internal/storage"
	"github.com/9ifrashaikh/distributed-system/pkg/models"
	"github.com/gorilla/mux"
)

type APIServer struct {
	store   *storage.FileStore
	router  *mux.Router
	tracker *AccessTracker
}

type AccessTracker struct {
	patterns []models.AccessPattern
}

func NewAPIServer(store *storage.FileStore) *APIServer {
	api := &APIServer{
		store:   store,
		router:  mux.NewRouter(),
		tracker: &AccessTracker{},
	}

	api.setupRoutes()
	return api
}

func (api *APIServer) setupRoutes() {
	api.router.HandleFunc("/objects", api.listObjects).Methods("GET")
	api.router.HandleFunc("/objects/{key}", api.getObject).Methods("GET")
	api.router.HandleFunc("/objects/{key}", api.putObject).Methods("PUT")
	api.router.HandleFunc("/objects/{key}", api.deleteObject).Methods("DELETE")
	api.router.HandleFunc("/stats", api.getStats).Methods("GET")
	api.router.HandleFunc("/health", api.healthCheck).Methods("GET")
}

func (api *APIServer) putObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	obj, err := api.store.Put(key, r.Body, contentType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Track access pattern
	api.trackAccess(obj.ID, "write", r.Header.Get("User-ID"), obj.Size)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj)
}

func (api *APIServer) getObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	reader, obj, err := api.store.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer reader.Close()

	// Track access pattern
	api.trackAccess(obj.ID, "read", r.Header.Get("User-ID"), obj.Size)

	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("ETag", obj.Checksum)

	io.Copy(w, reader)
}

func (api *APIServer) deleteObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := api.store.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (api *APIServer) listObjects(w http.ResponseWriter, r *http.Request) {
	objects := api.store.List()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(objects)
}

func (api *APIServer) getStats(w http.ResponseWriter, r *http.Request) {
	objects := api.store.List()

	stats := map[string]interface{}{
		"total_objects":     len(objects),
		"total_size":        calculateTotalSize(objects),
		"tier_distribution": calculateTierDistribution(objects),
		"access_patterns":   api.tracker.patterns,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (api *APIServer) healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (api *APIServer) trackAccess(objectID, operation, userID string, size int64) {
	pattern := models.AccessPattern{
		ObjectID:   objectID,
		AccessTime: time.Now(),
		Operation:  operation,
		UserID:     userID,
		Size:       size,
	}
	api.tracker.patterns = append(api.tracker.patterns, pattern)
}

func (api *APIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.router.ServeHTTP(w, r)
}

func calculateTotalSize(objects map[string]*models.StorageObject) int64 {
	var total int64
	for _, obj := range objects {
		total += obj.Size
	}
	return total
}

func calculateTierDistribution(objects map[string]*models.StorageObject) map[string]int {
	distribution := make(map[string]int)
	for _, obj := range objects {
		distribution[obj.StorageTier]++
	}
	return distribution
}
