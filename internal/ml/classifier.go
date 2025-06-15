package ml

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/9ifrashaikh/distributed-system/pkg/models"
)

type DataClassifier struct {
	accessPatterns []models.AccessPattern
	tieringRules   TieringRules
}

type TieringRules struct {
	HotTierDays     int   `json:"hot_tier_days"`
	WarmTierDays    int   `json:"warm_tier_days"`
	AccessThreshold int64 `json:"access_threshold"`
	SizeThreshold   int64 `json:"size_threshold"`
}

type ObjectScore struct {
	ObjectID   string             `json:"object_id"`
	Score      float64            `json:"score"`
	Prediction string             `json:"prediction"`
	Confidence float64            `json:"confidence"`
	Features   map[string]float64 `json:"features"`
}

func NewDataClassifier() *DataClassifier {
	return &DataClassifier{
		accessPatterns: make([]models.AccessPattern, 0),
		tieringRules: TieringRules{
			HotTierDays:     7,           // Objects accessed in last 7 days = hot
			WarmTierDays:    30,          // Objects accessed in last 30 days = warm
			AccessThreshold: 10,          // Minimum access count for hot tier
			SizeThreshold:   1024 * 1024, // 1MB threshold for size-based decisions
		},
	}
}

func (dc *DataClassifier) AddAccessPattern(pattern models.AccessPattern) {
	dc.accessPatterns = append(dc.accessPatterns, pattern)
}

func (dc *DataClassifier) ClassifyObjects(objects map[string]*models.StorageObject) ([]ObjectScore, error) {
	scores := make([]ObjectScore, 0, len(objects))

	for _, obj := range objects {
		score := dc.calculateObjectScore(obj)
		scores = append(scores, score)
	}

	// Sort by score (highest first)
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	return scores, nil
}

func (dc *DataClassifier) calculateObjectScore(obj *models.StorageObject) ObjectScore {
	now := time.Now()

	// Feature extraction
	features := make(map[string]float64)

	// Recency feature (days since last access)
	daysSinceAccess := now.Sub(obj.LastAccess).Hours() / 24
	features["days_since_access"] = daysSinceAccess

	// Frequency feature (access count)
	features["access_count"] = float64(obj.AccessCount)

	// Size feature (normalized)
	features["size_mb"] = float64(obj.Size) / (1024 * 1024)

	// Age feature (days since creation)
	daysSinceCreation := now.Sub(obj.CreatedAt).Hours() / 24
	features["days_since_creation"] = daysSinceCreation

	// Access frequency (accesses per day)
	if daysSinceCreation > 0 {
		features["access_frequency"] = features["access_count"] / daysSinceCreation
	} else {
		features["access_frequency"] = features["access_count"]
	}

	// Calculate composite score
	score := dc.calculateCompositeScore(features)

	// Determine tier prediction
	prediction, confidence := dc.predictTier(features, score)

	return ObjectScore{
		ObjectID:   obj.ID,
		Score:      score,
		Prediction: prediction,
		Confidence: confidence,
		Features:   features,
	}
}

func (dc *DataClassifier) calculateCompositeScore(features map[string]float64) float64 {
	// Weights for different features (can be tuned)
	weights := map[string]float64{
		"recency_weight":   0.4, // How recently accessed
		"frequency_weight": 0.3, // How often accessed
		"size_weight":      0.2, // Size considerations
		"age_weight":       0.1, // Age of the object
	}

	// Normalize and score each feature
	recencyScore := math.Max(0, 1.0-features["days_since_access"]/30.0) // Decay over 30 days
	frequencyScore := math.Min(1.0, features["access_frequency"]*10)    // Cap at reasonable frequency
	sizeScore := 1.0 / (1.0 + features["size_mb"]/100)                  // Smaller files scored higher
	ageScore := math.Max(0, 1.0-features["days_since_creation"]/365.0)  // Newer files scored higher

	// Weighted combination
	score := weights["recency_weight"]*recencyScore +
		weights["frequency_weight"]*frequencyScore +
		weights["size_weight"]*sizeScore +
		weights["age_weight"]*ageScore

	return score
}

func (dc *DataClassifier) predictTier(features map[string]float64, score float64) (string, float64) {
	daysSinceAccess := features["days_since_access"]
	accessCount := features["access_count"]

	// Rule-based classification with confidence
	if daysSinceAccess <= float64(dc.tieringRules.HotTierDays) &&
		accessCount >= float64(dc.tieringRules.AccessThreshold) {
		return "hot", 0.9
	}

	if daysSinceAccess <= float64(dc.tieringRules.WarmTierDays) {
		confidence := 0.7 + (0.2 * (1.0 - daysSinceAccess/float64(dc.tieringRules.WarmTierDays)))
		return "warm", confidence
	}

	// Cold tier
	confidence := 0.8 + (0.2 * math.Min(1.0, daysSinceAccess/90.0))
	return "cold", confidence
}

func (dc *DataClassifier) GetRecommendations(objects map[string]*models.StorageObject) ([]TieringRecommendation, error) {
	scores, err := dc.ClassifyObjects(objects)
	if err != nil {
		return nil, err
	}

	recommendations := make([]TieringRecommendation, 0)

	for _, score := range scores {
		obj := objects[findObjectByID(objects, score.ObjectID)]
		if obj != nil && obj.StorageTier != score.Prediction {
			rec := TieringRecommendation{
				ObjectID:         score.ObjectID,
				ObjectKey:        obj.Key,
				CurrentTier:      obj.StorageTier,
				RecommendedTier:  score.Prediction,
				Confidence:       score.Confidence,
				Reason:           dc.generateReason(score.Features, score.Prediction),
				EstimatedSavings: dc.calculateSavings(obj, score.Prediction),
			}
			recommendations = append(recommendations, rec)
		}
	}

	return recommendations, nil
}

type TieringRecommendation struct {
	ObjectID         string  `json:"object_id"`
	ObjectKey        string  `json:"object_key"`
	CurrentTier      string  `json:"current_tier"`
	RecommendedTier  string  `json:"recommended_tier"`
	Confidence       float64 `json:"confidence"`
	Reason           string  `json:"reason"`
	EstimatedSavings float64 `json:"estimated_savings"`
}

func (dc *DataClassifier) generateReason(features map[string]float64, prediction string) string {
	switch prediction {
	case "hot":
		return fmt.Sprintf("Recently accessed (%.1f days ago) with high frequency (%.1f accesses)",
			features["days_since_access"], features["access_count"])
	case "warm":
		return fmt.Sprintf("Moderate access pattern (%.1f days since last access)",
			features["days_since_access"])
	case "cold":
		return fmt.Sprintf("Infrequently accessed (%.1f days ago) - suitable for archival",
			features["days_since_access"])
	default:
		return "Unknown classification reason"
	}
}

func (dc *DataClassifier) calculateSavings(obj *models.StorageObject, recommendedTier string) float64 {
	// Simple cost model (dollars per GB per month)
	costs := map[string]float64{
		"hot":  0.023, // High-performance storage
		"warm": 0.012, // Standard storage
		"cold": 0.004, // Archive storage
	}

	currentCost := costs[obj.StorageTier]
	newCost := costs[recommendedTier]

	sizeGB := float64(obj.Size) / (1024 * 1024 * 1024)
	monthlySavings := (currentCost - newCost) * sizeGB

	return monthlySavings
}

func findObjectByID(objects map[string]*models.StorageObject, id string) string {
	for key, obj := range objects {
		if obj.ID == id {
			return key
		}
	}
	return ""
}
