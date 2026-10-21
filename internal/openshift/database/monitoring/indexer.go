package monitoring

import (
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InstanceIDIndexFunc is a cache.IndexFunc that indexes objects by their
// dcm-instance-id label value.
func InstanceIDIndexFunc(obj any) ([]string, error) {
	meta, ok := obj.(metav1.Object)
	if !ok {
		return []string{}, nil
	}
	id := ExtractInstanceID(meta)
	if id == "" {
		return []string{}, nil
	}
	return []string{id}, nil
}

// ExtractInstanceID returns the dcm-instance-id label value from a
// Kubernetes object's metadata.
func ExtractInstanceID(obj metav1.Object) string {
	labels := obj.GetLabels()
	return labels[dcm.LabelInstanceID]
}
