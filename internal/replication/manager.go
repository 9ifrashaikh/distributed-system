package replication

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/yourusername/distributed-storage-system/internal/cluster"
	"github.com/yourusername/distributed-storage-system/pkg/models"
)

type ReplicationManager struct {
	clusterManager      *cluster.ClusterManager
	replicationFactor   int
	client              *http.Client
	pendingReplications sync.Map
}

type ReplicationTask struct {
	ObjectID    string     `json:"object_id"`
	ObjectKey   string     `json:"object_key"`
	SourceNode  string     `json:"source_node"`
	TargetNodes []string   `json:"target_nodes"`
	Status      string     `json:"status"` // pending, in_progress, completed, failed
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

func NewReplicationManager(cm *cluster.ClusterManager, replicationFactor int) *ReplicationManager {
	return &ReplicationManager{
		clusterManager:    cm,
		replicationFactor: replicationFactor,
		client:            &http.Client{Timeout: 30 * time.Second},
	}
}

func (rm *ReplicationManager) ReplicateObject(obj *models.StorageObject, data io.Reader) error {
	// Select target nodes for replication
	targetNodes := rm.clusterManager.SelectNodesForReplication(rm.replicationFactor)
	if len(targetNodes) == 0 {
		return fmt.Errorf("no healthy nodes available for replication")
	}

	// Create replication task
	task := &ReplicationTask{
		ObjectID:    obj.ID,
		ObjectKey:   obj.Key,
		SourceNode:  rm.clusterManager.GetCurrentNode().ID,
		TargetNodes: make([]string, len(targetNodes)),
		Status:      "pending",
		CreatedAt:   time.Now(),
	}

	for i, node := range targetNodes {
		task.TargetNodes[i] = node.ID
	}

	rm.pendingReplications.Store(obj.ID, task)

	// Start replication in background
	go rm.executeReplication(task, obj, data)

	return nil
}

func (rm *ReplicationManager) executeReplication(task *ReplicationTask, obj *models.StorageObject, data io.Reader) {
	task.Status = "in_progress"
	rm.pendingReplications.Store(task.ObjectID, task)

	// Read data into buffer for multiple replications
	buffer := &bytes.Buffer{}
	_, err := io.Copy(buffer, data)
	if err != nil {
		rm.markTaskFailed(task, fmt.Sprintf("Failed to buffer data: %v", err))
		return
	}

	var wg sync.WaitGroup
	successCount := 0
	var mutex sync.Mutex

	// Replicate to each target node
	for _, nodeID := range task.TargetNodes {
		wg.Add(1)
		go func(nID string) {
			defer wg.Done()

			if rm.replicateToNode(nID, obj, bytes.NewReader(buffer.Bytes())) {
				mutex.Lock()
				successCount++
				mutex.Unlock()
				log.Printf("Successfully replicated object %s to node %s", obj.Key, nID)
			} else {
				log.Printf("Failed to replicate object %s to node %s", obj.Key, nID)
			}
		}(nodeID)
	}

	wg.Wait()

	// Update task status
	if successCount > 0 {
		task.Status = "completed"
		now := time.Now()
		task.CompletedAt = &now
		log.Printf("Replication completed for object %s (%d/%d nodes successful)",
			obj.Key, successCount, len(task.TargetNodes))
	} else {
		rm.markTaskFailed(task, "Failed to replicate to any target node")
	}

	rm.pendingReplications.Store(task.ObjectID, task)
}

func (rm *ReplicationManager) replicateToNode(nodeID string, obj *models.StorageObject, data io.Reader) bool {
	// Get node information
	nodes := rm.clusterManager.GetHealthyNodes()
	var targetNode *cluster.Node
	for _, node := range nodes {
		if node.ID == nodeID {
			targetNode = node
			break
		}
	}

	if targetNode == nil {
		return false
	}

	// Create replication request
	url := fmt.Sprintf("http://%s/internal/replicate/%s", targetNode.Address, obj.Key)

	req, err := http.NewRequest("PUT", url, data)
	if err != nil {
		return false
	}

	req.Header.Set("Content-Type", obj.ContentType)
	req.Header.Set("X-Object-ID", obj.ID)
	req.Header.Set("X-Checksum", obj.Checksum)
	req.Header.Set("X-Replication-Source", rm.clusterManager.GetCurrentNode().ID)

	resp, err := rm.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

func (rm *ReplicationManager) markTaskFailed(task *ReplicationTask, errorMsg string) {
	task.Status = "failed"
	task.Error = errorMsg
	now := time.Now()
	task.CompletedAt = &now
}

func (rm *ReplicationManager) GetReplicationStatus(objectID string) (*ReplicationTask, bool) {
	task, exists := rm.pendingReplications.Load(objectID)
	if !exists {
		return nil, false
	}
	return task.(*ReplicationTask), true
}

func (rm *ReplicationManager) GetAllReplicationTasks() []*ReplicationTask {
	var tasks []*ReplicationTask
	rm.pendingReplications.Range(func(key, value interface{}) bool {
		tasks = append(tasks, value.(*ReplicationTask))
		return true
	})
	return tasks
}
