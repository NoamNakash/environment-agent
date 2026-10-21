package monitoring

import (
	"encoding/json"
	"fmt"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/google/uuid"
)

type cloudEvent struct {
	SpecVersion     string         `json:"specversion"`
	ID              string         `json:"id"`
	Source          string         `json:"source"`
	Type            string         `json:"type"`
	Subject         string         `json:"subject"`
	Time            string         `json:"time"`
	DataContentType string         `json:"datacontenttype"`
	Data            cloudEventData `json:"data"`
}

type cloudEventData struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// NewStatusCloudEvent constructs a CloudEvents v1.0 JSON payload for a status
// change notification.
func NewStatusCloudEvent(subject, providerName, instanceID string, status v1alpha1.DatabaseStatus, message string) ([]byte, error) {
	ce := cloudEvent{
		SpecVersion:     "1.0",
		ID:              uuid.NewString(),
		Source:          "dcm/providers/" + providerName,
		Type:            "dcm.status.database",
		Subject:         subject,
		Time:            time.Now().UTC().Format(time.RFC3339),
		DataContentType: "application/json",
		Data: cloudEventData{
			ID:      instanceID,
			Status:  string(status),
			Message: message,
		},
	}
	data, err := json.Marshal(ce)
	if err != nil {
		return nil, fmt.Errorf("marshaling status CloudEvent: %w", err)
	}
	return data, nil
}
