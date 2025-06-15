package models

import (
	"time"
)

type StorageObject struct {
	ID          string            `json:"id"`
	Key         string            `json:"key"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	Checksum    string            `json:"checksum"` //for file integrating SHA256 SOMEWHAT
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	AccessCount int64             `json:"access_count"`
	LastAccess  time.Time         `json:"last_access"`
	Metadata    map[string]string `json:"metadata"`
	StorageTier string            `json:"storage_tier"` // hot, warm, cold
	Replicas    []ReplicaInfo     `json:"replicas"`
}

// STRUCTURE NO 2
type ReplicaInfo struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path"`
	Status   string `json:"status"` // active, syncing, failed
}

type AccessPattern struct {
	ObjectID   string    `json:"object_id"`
	AccessTime time.Time `json:"access_time"`
	Operation  string    `json:"operation"` // read, write, delete
	UserID     string    `json:"user_id"`
	Size       int64     `json:"size"`
}
