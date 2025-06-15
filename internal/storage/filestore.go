package storage //it handles actual file operations, like saving, retrieving, and deleting files.

//backend for distributed storage system
import (
	"crypto/md5" //To generate a unique checksum of file content.
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync" //To ensure thread-safe access using mutexes.
	"time"

	"github.com/9ifrashaikh/distributed-system/pkg/models"
)

type FileStore struct {
	basePath     string
	metadataPath string // json files
	objects      map[string]*models.StorageObject
	mutex        sync.RWMutex
}

func NewFileStore(basePath string) *FileStore {
	fs := &FileStore{
		basePath:     basePath,
		metadataPath: filepath.Join(basePath, "metadata"),
		objects:      make(map[string]*models.StorageObject),
	}

	// Create directories
	os.MkdirAll(basePath, 0755)
	os.MkdirAll(fs.metadataPath, 0755)

	// Load existing metadata
	fs.loadMetadata()

	return fs
}

// This is how new file uploads are handled.
// see about IAM policies and access control later
// It generates a unique ID for each file, saves it to the filesystem, and updates metadata.
// method for uploading files to the storage system
func (fs *FileStore) Put(key string, data io.Reader, contentType string) (*models.StorageObject, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	// Generate object ID
	objectID := fmt.Sprintf("%x", md5.Sum([]byte(key+time.Now().String())))

	// Create file path
	filePath := filepath.Join(fs.basePath, objectID)

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	// Calculate checksum while writing
	hasher := md5.New()
	writer := io.MultiWriter(file, hasher)

	size, err := io.Copy(writer, data)
	if err != nil {
		os.Remove(filePath)
		return nil, fmt.Errorf("failed to write data: %v", err)
	}

	checksum := fmt.Sprintf("%x", hasher.Sum(nil))

	// Create storage object
	obj := &models.StorageObject{
		ID:          objectID,
		Key:         key,
		Size:        size,
		ContentType: contentType,
		Checksum:    checksum,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		AccessCount: 0,
		LastAccess:  time.Now(),
		StorageTier: "hot",
		Replicas: []models.ReplicaInfo{
			{
				NodeID:   "node-1", // Current node
				FilePath: filePath,
				Status:   "active",
			},
		},
	}

	fs.objects[key] = obj
	fs.saveMetadata()

	return obj, nil
}

//retreiving th edata from the storage system

func (fs *FileStore) Get(key string) (io.ReadCloser, *models.StorageObject, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	obj, exists := fs.objects[key]
	if !exists {
		return nil, nil, fmt.Errorf("object not found: %s", key)
	}

	// Update access statistics
	obj.AccessCount++
	obj.LastAccess = time.Now()
	fs.saveMetadata()

	// Open file
	file, err := os.Open(obj.Replicas[0].FilePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %v", err)
	}

	return file, obj, nil
}

// This method deletes a file from the storage system and removes its metadata.

func (fs *FileStore) Delete(key string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	obj, exists := fs.objects[key]
	if !exists {
		return fmt.Errorf("object not found: %s", key)
	}

	// Remove file
	for _, replica := range obj.Replicas {
		os.Remove(replica.FilePath)
	}

	delete(fs.objects, key)
	fs.saveMetadata()

	return nil
}

// This method lists all objects in the storage system, returning their metadata.

func (fs *FileStore) List() map[string]*models.StorageObject {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	result := make(map[string]*models.StorageObject)
	for k, v := range fs.objects {
		result[k] = v
	}
	return result
}

// This method retrieves the metadata of a specific object by its key.

func (fs *FileStore) saveMetadata() {
	data, _ := json.MarshalIndent(fs.objects, "", "  ")
	os.WriteFile(filepath.Join(fs.metadataPath, "objects.json"), data, 0644)
}

func (fs *FileStore) loadMetadata() {
	data, err := os.ReadFile(filepath.Join(fs.metadataPath, "objects.json"))
	if err != nil {
		return
	}
	json.Unmarshal(data, &fs.objects)
}
