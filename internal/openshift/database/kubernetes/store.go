// Package kubernetes implements the database store using Kubernetes resources.
package kubernetes

import (
	"context"
	"fmt"
	"log/slog"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	oshutil "github.com/dcm-project/environment-agent/internal/openshift/util"
	"github.com/dcm-project/environment-agent/internal/ptr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const (
	dataVolumeName = "pgdata"
	InstanceLabel  = "cnpg.io/instanceName"
)

var ClustersResource = schema.GroupVersionResource{
	Group:    "postgresql.cnpg.io",
	Version:  "v1",
	Resource: "clusters",
}

// CnpgDatabaseStore implements store.DatabaseRepository backed by CNPG
// Clusters, and Kubernetes Pods and Services.
type CnpgDatabaseStore struct {
	client        kubernetes.Interface
	dynamicClient dynamic.Interface
	cfg           CnpgK8sConfig
	logger        *slog.Logger
}

// NewCnpgDatabaseStore creates a new CnpgDatabaseStore with the given client, config, and logger.
func NewCnpgDatabaseStore(client kubernetes.Interface, dynamicClient dynamic.Interface, cfg CnpgK8sConfig, logger *slog.Logger) *CnpgDatabaseStore {
	return &CnpgDatabaseStore{
		client:        client,
		dynamicClient: dynamicClient,
		cfg:           cfg,
		logger:        logger,
	}
}

// CheckHealth verifies the backing Kubernetes cluster is reachable by calling
// the API server's version discovery endpoint.
func (s *CnpgDatabaseStore) CheckHealth(ctx context.Context) error {
	if err := oshutil.ServerVersion(ctx, s.client); err != nil {
		return fmt.Errorf("kubernetes health check failed: %w", err)
	}
	return nil
}

// findCluster looks up the single cluster for a database instance.
func (s *CnpgDatabaseStore) findCluster(ctx context.Context, databaseId string) (*cnpgv1.Cluster, error) {
	clusters, err := s.dynamicClient.Resource(ClustersResource).Namespace(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: instanceSelector(databaseId),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list clusters: %w", err)
	}
	if len(clusters.Items) == 0 {
		return nil, &store.NotFoundError{ID: databaseId}
	}
	if len(clusters.Items) > 1 {
		return nil, &store.ConflictError{Message: fmt.Sprintf("multiple clusters found for database %q", databaseId)}
	}

	var cluster cnpgv1.Cluster
	if err = runtime.DefaultUnstructuredConverter.FromUnstructured(clusters.Items[0].Object, &cluster); err != nil {
		return nil, fmt.Errorf("decoding cluster for database %q: %w", databaseId, err)
	}

	return &cluster, nil
}

// buildDatabase reconstructs an API database from a CNPG Cluster and enriches
// it with runtime data from the K8s cluster.
func (s *CnpgDatabaseStore) buildDatabase(ctx context.Context, cluster *cnpgv1.Cluster, instanceId string) (db *v1alpha1.Database, err error) {
	db = databaseFromCluster(cluster, instanceId)
	if err := s.enrichFromCluster(ctx, db, cluster, instanceId); err != nil {
		return nil, err
	}

	return
}

// MatchReplicas matches the pods and pvcs into temporary databaseReplica objects
// that each contain the pod and it's matching pgdata volume
func MatchReplicas(pods []corev1.Pod, pvcs []corev1.PersistentVolumeClaim) (replicas []databaseReplica, err error) {
	for _, pvc := range pvcs {
		podName, ok := pvc.Labels[InstanceLabel]
		if !ok {
			return nil, &store.ConflictError{Message: "database has mismatched pods/pvcs"}
		}
		matched := false
		for idx, pod := range pods {
			if pod.Name == podName {
				replicas = append(replicas, databaseReplica{ptr.To(pod), ptr.To(pvc)})
				pods = append(pods[:idx], pods[idx+1:]...)
				matched = true
				break
			}
		}
		if !matched {
			return nil, &store.ConflictError{Message: "database has mismatched pods/pvcs"}
		}
	}
	if len(pods) > 0 {
		return nil, &store.ConflictError{Message: "database has mismatched pods/pvcs"}
	}
	return
}

// enrichFromCluster enriches a Database with runtime data from Pods and Services.
func (s *CnpgDatabaseStore) enrichFromCluster(
	ctx context.Context,
	db *v1alpha1.Database,
	cluster *cnpgv1.Cluster,
	instanceID string,
) error {
	pods, err := s.client.CoreV1().Pods(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: instanceSelector(instanceID),
	})
	if err != nil {
		return fmt.Errorf("listing pods for database %q: %w", instanceID, err)
	}

	pvcs, err := s.client.CoreV1().PersistentVolumeClaims(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: instanceSelector(instanceID),
	})
	if err != nil {
		return fmt.Errorf("listing pvcs for database %q: %w", instanceID, err)
	}

	// for v1, updates are not supported, so the case of rolling update is irrelevant
	// checks deletion timestamp to see if the cluster is in the process of being deleted,
	// where it would make sense for pods not to match PVCs
	if len(pods.Items) != len(pvcs.Items) && cluster.DeletionTimestamp == nil {
		return &store.ConflictError{Message: "database has mismatched pods/pvcs"}
	}

	if cluster.DeletionTimestamp == nil {
		if len(pods.Items) == 0 {
			db.Status = ptr.To(v1alpha1.PENDING)
			if t := latestClusterTransitionTime(*cluster); t != nil {
				db.UpdateTime = t
			}
		} else {
			replicas, err := MatchReplicas(pods.Items, pvcs.Items)
			if err != nil {
				return err
			}
			enrichWithReplicas(db, replicas)
		}
	} else {
		db.Status = ptr.To(v1alpha1.DELETING)
		if t := latestClusterTransitionTime(*cluster); t != nil {
			db.UpdateTime = t
		}
		for _, pod := range pods.Items {
			for _, cond := range pod.Status.Conditions {
				if !cond.LastTransitionTime.IsZero() && (db.UpdateTime == nil || cond.LastTransitionTime.After(*db.UpdateTime)) {
					db.UpdateTime = &cond.LastTransitionTime.Time
				}
			}
		}
	}

	credentialsSecretName := credentialSecretFromCluster(cluster)
	credentials, err := s.client.CoreV1().Secrets(s.cfg.Namespace).Get(ctx, credentialsSecretName, metav1.GetOptions{})
	if err != nil {
		credentials = nil
	}

	primarySvcName := serviceFromCluster(cluster)
	primarySvc, err := s.client.CoreV1().Services(s.cfg.Namespace).Get(ctx, primarySvcName, metav1.GetOptions{})
	if err != nil {
		primarySvc = nil
	}

	var nodeAddress string
	if primarySvc != nil && primarySvc.Spec.Type == corev1.ServiceTypeNodePort {
		nodes, nodeErr := s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if nodeErr == nil {
			nodeAddress = nodeAddressFromNodes(nodes.Items)
		}
	}

	if err := populateConnectionString(db, credentials, primarySvc, databaseNameFromCluster(cluster), nodeAddress); err != nil {
		return &store.InvalidArgumentError{Message: err.Error()}
	}

	return nil
}
