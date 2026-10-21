package kubernetes

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// Create creates a new container backed by a Kubernetes Deployment (and
// optionally a Service when ports have non-none visibility).
func (s *CnpgDatabaseStore) Create(ctx context.Context, spec v1alpha1.DatabaseSpec, id string) (*v1alpha1.Database, error) {
	labels := dcmLabels(id)
	if spec.Metadata.Labels != nil {
		labels = mergeLabels(labels, *spec.Metadata.Labels)
	}

	// Check for duplicate dcm-instance-id.
	_, err := s.findCluster(ctx, id)
	if err != nil && !store.IsNotFound(err) {
		return nil, fmt.Errorf("checking for existing database %q: %w", id, err)
	} else if err == nil {
		return nil, &store.ConflictError{Message: fmt.Sprintf("database with instance ID %q already exists", id)}
	}

	// Create Cluster.
	cluster, err := buildCluster(spec, id, s.cfg, labels)
	if err != nil {
		return nil, err
	}

	// Create a credentials secret if needed. Treat AlreadyExists as acceptable:
	// a prior failed attempt may have left the secret behind. Mark it for
	// cleanup so any subsequent failure in this call removes the leftover.
	var secretCreated bool
	if password, exists := databasePasswordFromProviderHints(spec.ProviderHints); exists {
		username := databaseUserFromBootstrap(cluster.Spec.Bootstrap)
		_, err := s.client.CoreV1().Secrets(s.cfg.Namespace).Create(ctx, newCredentialsSecret(cluster.Name, username, password, labels), metav1.CreateOptions{})
		if !apierrors.IsAlreadyExists(err) {
			if err != nil {
				return nil, fmt.Errorf("creating credentials secret for database %q: %w", id, err)
			}
			secretCreated = true
		}
	}

	// deleteSecret removes the credentials secret using a fresh context so that
	// a cancelled request context does not prevent cleanup.
	deleteSecret := func() {
		if !secretCreated {
			return
		}
		_ = s.client.CoreV1().Secrets(s.cfg.Namespace).Delete(
			context.Background(),
			fmt.Sprintf(customCredentialsSecretNamePattern, cluster.Name),
			metav1.DeleteOptions{},
		)
	}

	clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
	if err != nil {
		deleteSecret()
		return nil, fmt.Errorf("encoding cluster for database %q: %w", id, err)
	}
	clusterUnstructured := &unstructured.Unstructured{Object: clusterMap}
	clusterUnstructured, err = s.dynamicClient.Resource(ClustersResource).Namespace(s.cfg.Namespace).Create(ctx, clusterUnstructured, metav1.CreateOptions{})
	if err != nil {
		deleteSecret()
		if apierrors.IsAlreadyExists(err) {
			return nil, &store.ConflictError{Message: fmt.Sprintf("cluster %q already exists", spec.Metadata.Name)}
		}
		return nil, fmt.Errorf("creating cluster for database %q: %w", id, err)
	}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(clusterUnstructured.Object, cluster)
	if err != nil {
		deleteSecret()
		return nil, fmt.Errorf("decoding created cluster for database %q: %w", id, err)
	}

	return newDatabaseResult(spec, id, s.cfg.Namespace), nil
}

// newDatabaseResult stamps server-assigned fields onto a user-provided spec.
func newDatabaseResult(spec v1alpha1.DatabaseSpec, id, namespace string) *v1alpha1.Database {
	now := time.Now()
	status := v1alpha1.PENDING
	path := fmt.Sprintf("databases/%s", id)

	spec.Metadata.Namespace = &namespace

	return &v1alpha1.Database{
		Id:         &id,
		Path:       &path,
		Status:     &status,
		CreateTime: &now,
		UpdateTime: &now,
		Spec:       spec,
	}
}
