package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type Node struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	Status   string    `json:"status"` // healthy, unhealthy, unknown
	LastSeen time.Time `json:"last_seen"`
	Load     float64   `json:"load"`     // Current load (0.0 to 1.0)
	Capacity int64     `json:"capacity"` // Storage capacity in bytes
	Used     int64     `json:"used"`     // Used storage in bytes
}

type ClusterManager struct {
	nodes        map[string]*Node
	currentNode  *Node
	mutex        sync.RWMutex
	healthTicker *time.Ticker
}

func NewClusterManager(nodeID, nodeAddress string) *ClusterManager {
	cm := &ClusterManager{
		nodes: make(map[string]*Node),
		currentNode: &Node{
			ID:       nodeID,
			Address:  nodeAddress,
			Status:   "healthy",
			LastSeen: time.Now(),
			Load:     0.0,
			Capacity: 10 * 1024 * 1024 * 1024, // 10GB default
			Used:     0,
		},
	}

	cm.nodes[nodeID] = cm.currentNode
	cm.startHealthCheck()

	return cm
}

func (cm *ClusterManager) RegisterNode(node *Node) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	node.LastSeen = time.Now()
	cm.nodes[node.ID] = node

	log.Printf("Node registered: %s (%s)", node.ID, node.Address)
}

func (cm *ClusterManager) GetHealthyNodes() []*Node {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	var healthy []*Node
	for _, node := range cm.nodes {
		if node.Status == "healthy" {
			healthy = append(healthy, node)
		}
	}

	return healthy
}

func (cm *ClusterManager) SelectNodeForWrite() *Node {
	nodes := cm.GetHealthyNodes()
	if len(nodes) == 0 {
		return nil
	}

	// Select node with lowest load
	var bestNode *Node
	lowestLoad := 1.0

	for _, node := range nodes {
		utilization := float64(node.Used) / float64(node.Capacity)
		if utilization < lowestLoad {
			lowestLoad = utilization
			bestNode = node
		}
	}

	return bestNode
}

func (cm *ClusterManager) SelectNodesForReplication(count int) []*Node {
	nodes := cm.GetHealthyNodes()
	if len(nodes) <= count {
		return nodes
	}

	// Simple selection - could be improved with rack awareness, etc.
	selected := make([]*Node, 0, count)
	for i := 0; i < count && i < len(nodes); i++ {
		selected = append(selected, nodes[i])
	}

	return selected
}

func (cm *ClusterManager) startHealthCheck() {
	cm.healthTicker = time.NewTicker(30 * time.Second)

	go func() {
		for range cm.healthTicker.C {
			cm.performHealthCheck()
		}
	}()
}

func (cm *ClusterManager) performHealthCheck() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	now := time.Now()

	for nodeID, node := range cm.nodes {
		if nodeID == cm.currentNode.ID {
			continue // Skip self
		}

		// Check if node is stale
		if now.Sub(node.LastSeen) > 60*time.Second {
			node.Status = "unhealthy"
			log.Printf("Node marked unhealthy: %s", nodeID)
			continue
		}

		// Ping node
		if cm.pingNode(node) {
			node.Status = "healthy"
			node.LastSeen = now
		} else {
			node.Status = "unhealthy"
		}
	}
}

func (cm *ClusterManager) pingNode(node *Node) bool {
	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Get(fmt.Sprintf("http://%s/health", node.Address))
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

func (cm *ClusterManager) GetClusterStats() map[string]interface{} {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	totalNodes := len(cm.nodes)
	healthyNodes := 0
	totalCapacity := int64(0)
	totalUsed := int64(0)

	for _, node := range cm.nodes {
		if node.Status == "healthy" {
			healthyNodes++
		}
		totalCapacity += node.Capacity
		totalUsed += node.Used
	}

	return map[string]interface{}{
		"total_nodes":    totalNodes,
		"healthy_nodes":  healthyNodes,
		"total_capacity": totalCapacity,
		"total_used":     totalUsed,
		"utilization":    float64(totalUsed) / float64(totalCapacity),
		"nodes":          cm.nodes,
	}
}

// HTTP handlers for cluster management
func (cm *ClusterManager) HandleNodeRegistration(w http.ResponseWriter, r *http.Request) {
	var node Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		http.Error(w, "Invalid node data", http.StatusBadRequest)
		return
	}

	cm.RegisterNode(&node)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
}

func (cm *ClusterManager) HandleClusterStatus(w http.ResponseWriter, r *http.Request) {
	stats := cm.GetClusterStats()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
