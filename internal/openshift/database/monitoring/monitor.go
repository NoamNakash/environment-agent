package monitoring

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	k8sutils "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const instanceIDIndex = "instanceID"

// StatusMonitor watches Cluster, PVC and Pod resources for DCM-managed
// containers and publishes status change events via a StatusPublisher.
type StatusMonitor struct {
	// dependencies
	client        kubernetes.Interface
	dynamicClient dynamic.Interface
	publisher     StatusPublisher

	// configuration
	cfg MonitorConfig

	// infrastructure
	logger *slog.Logger
}

// NewStatusMonitor creates a new StatusMonitor.
func NewStatusMonitor(client kubernetes.Interface, dynamicClient dynamic.Interface, cfg MonitorConfig, publisher StatusPublisher, logger *slog.Logger) *StatusMonitor {
	if client == nil {
		panic("monitoring: client is required")
	}
	if dynamicClient == nil {
		panic("monitoring: dynamicClient is required")
	}
	if publisher == nil {
		panic("monitoring: publisher is required")
	}
	if logger == nil {
		panic("monitoring: logger is required")
	}
	return &StatusMonitor{
		client:        client,
		dynamicClient: dynamicClient,
		cfg:           cfg,
		publisher:     publisher,
		logger:        logger,
	}
}

// Start begins watching for resource changes. It blocks until ctx is cancelled.
func (m *StatusMonitor) Start(ctx context.Context) error {
	selector := dcm.Selector()

	dynamicfactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		m.dynamicClient,
		m.cfg.ResyncPeriod,
		m.cfg.Namespace,
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector
		},
	)

	factory := informers.NewSharedInformerFactoryWithOptions(
		m.client,
		m.cfg.ResyncPeriod,
		informers.WithNamespace(m.cfg.Namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector
		}),
	)

	clusterInformer := dynamicfactory.ForResource(k8sutils.ClustersResource).Informer()
	podInformer := factory.Core().V1().Pods().Informer()
	pvcInformer := factory.Core().V1().PersistentVolumeClaims().Informer()

	if err := clusterInformer.AddIndexers(cache.Indexers{instanceIDIndex: InstanceIDIndexFunc}); err != nil {
		return fmt.Errorf("adding cluster indexer: %w", err)
	}
	if err := podInformer.AddIndexers(cache.Indexers{instanceIDIndex: InstanceIDIndexFunc}); err != nil {
		return fmt.Errorf("adding pod indexer: %w", err)
	}
	if err := pvcInformer.AddIndexers(cache.Indexers{instanceIDIndex: InstanceIDIndexFunc}); err != nil {
		return fmt.Errorf("adding pvc indexer: %w", err)
	}

	// publishCtx is independent of the monitor context so that events still
	// pending in the debouncer when ctx is cancelled can be flushed during
	// the bounded shutdown phase (see debouncer.Stop and defer order below).
	publishCtx, cancelPublish := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelPublish() // registered first → runs LAST (LIFO), after debouncer.Stop

	debouncer := NewDebouncer(
		time.Duration(m.cfg.DebounceMs)*time.Millisecond,
		func(event StatusEvent) {
			m.publishWithRetry(publishCtx, event)
		},
	)

	// Cluster event handlers.
	if _, err := clusterInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.handleClusterEvent(obj, podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
		UpdateFunc: func(_, newObj any) {
			m.handleClusterEvent(newObj, podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
		DeleteFunc: func(obj any) {
			m.handleClusterDelete(obj, podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
	}); err != nil {
		return fmt.Errorf("adding cluster event handler: %w", err)
	}

	// Pod event handlers.
	if _, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.handleReplicaEvent(obj, clusterInformer.GetIndexer(), podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
		UpdateFunc: func(_, newObj any) {
			m.handleReplicaEvent(newObj, clusterInformer.GetIndexer(), podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
		DeleteFunc: func(obj any) {
			m.handlePodDelete(obj, clusterInformer.GetIndexer(), podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
	}); err != nil {
		return fmt.Errorf("adding pod event handler: %w", err)
	}

	// Pvc event handlers.
	if _, err := pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.handleReplicaEvent(obj, clusterInformer.GetIndexer(), podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
		UpdateFunc: func(_, newObj any) {
			m.handleReplicaEvent(newObj, clusterInformer.GetIndexer(), podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
		DeleteFunc: func(obj any) {
			m.handlePvcDelete(obj, clusterInformer.GetIndexer(), podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)
		},
	}); err != nil {
		return fmt.Errorf("adding pvc event handler: %w", err)
	}

	factory.Start(ctx.Done())
	defer factory.Shutdown()

	dynamicfactory.Start(ctx.Done())
	defer dynamicfactory.Shutdown()
	defer debouncer.Stop()

	synced := factory.WaitForCacheSync(ctx.Done())
	if ctx.Err() == nil {
		for typ, ok := range synced {
			if !ok {
				m.logger.Error("cache sync failed, aborting start", "type", typ)
				return fmt.Errorf("cache sync failed for %v", typ)
			}
		}
		if ok := cache.WaitForCacheSync(ctx.Done(), clusterInformer.HasSynced); !ok {
			m.logger.Error("cache sync failed, aborting start", "type", "clusters")
			return fmt.Errorf("cache sync failed for %v", "clusters")
		}
	}

	// All three caches are now warm. Open the publication gate and reconcile
	// every known instance from the complete initial snapshot so no instance is
	// missed and no partial-cache status is ever published.
	debouncer.SetReady()
	sweepInstances(clusterInformer.GetIndexer(), podInformer.GetIndexer(), pvcInformer.GetIndexer(), debouncer)

	<-ctx.Done()

	return nil
}

func (m *StatusMonitor) handleClusterEvent(obj any, podIndexer, pvcIndexer cache.Indexer, debouncer *Debouncer) {
	meta, ok := obj.(metav1.Object)
	if !ok {
		return
	}
	instanceId := ExtractInstanceID(meta)
	if instanceId == "" {
		return
	}

	clusterUnstructured, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return // should not be reachable
	}

	var cluster cnpgv1.Cluster
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(clusterUnstructured.Object, &cluster)
	if err != nil {
		return // should not be reachable
	}

	pods := lookupPodsByIndex(podIndexer, instanceId)
	pvcs := lookupPvcsByIndex(pvcIndexer, instanceId)
	reconcileAndSubmit(instanceId, &cluster, pods, pvcs, debouncer)
}

func (m *StatusMonitor) handleClusterDelete(obj any, podIndexer, pvcIndexer cache.Indexer, debouncer *Debouncer) {
	instanceID := extractInstanceIDFromDelete(obj)
	if instanceID == "" {
		return
	}

	pods := lookupPodsByIndex(podIndexer, instanceID)
	pvcs := lookupPvcsByIndex(pvcIndexer, instanceID)
	reconcileAndSubmit(instanceID, nil, pods, pvcs, debouncer)
}

func (m *StatusMonitor) handleReplicaEvent(obj any, clusterIndexer, podIndexer, pvcIndexer cache.Indexer, debouncer *Debouncer) {
	meta, ok := obj.(metav1.Object)
	if !ok {
		return
	}
	instanceID := ExtractInstanceID(meta)
	if instanceID == "" {
		return
	}

	pods := lookupPodsByIndex(podIndexer, instanceID)
	pvcs := lookupPvcsByIndex(pvcIndexer, instanceID)
	cluster := lookupClusterByIndex(clusterIndexer, instanceID)
	reconcileAndSubmit(instanceID, cluster, pods, pvcs, debouncer)
}

func (m *StatusMonitor) handlePodDelete(obj any, clusterIndexer, podIndexer, pvcIndexer cache.Indexer, debouncer *Debouncer) {
	instanceID := extractInstanceIDFromDelete(obj)
	if instanceID == "" {
		return
	}

	cluster := lookupClusterByIndex(clusterIndexer, instanceID)
	pods := lookupPodsByIndex(podIndexer, instanceID)
	pvcs := lookupPvcsByIndex(pvcIndexer, instanceID)
	reconcileAndSubmit(instanceID, cluster, pods, pvcs, debouncer)
}

func (m *StatusMonitor) handlePvcDelete(obj any, clusterIndexer, podIndexer, pvcIndexer cache.Indexer, debouncer *Debouncer) {
	instanceID := extractInstanceIDFromDelete(obj)
	if instanceID == "" {
		return
	}

	cluster := lookupClusterByIndex(clusterIndexer, instanceID)
	pods := lookupPodsByIndex(podIndexer, instanceID)
	pvcs := lookupPvcsByIndex(pvcIndexer, instanceID)
	reconcileAndSubmit(instanceID, cluster, pods, pvcs, debouncer)
}

func extractInstanceIDFromDelete(obj any) string {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	meta, ok := obj.(metav1.Object)
	if !ok {
		return ""
	}
	return ExtractInstanceID(meta)
}

func reconcileAndSubmit(instanceID string, cluster *cnpgv1.Cluster, pods []*corev1.Pod, pvcs []*corev1.PersistentVolumeClaim, debouncer *Debouncer) {
	status, msg, publish := ReconcileStatus(cluster, pods, pvcs)
	if publish {
		debouncer.Submit(instanceID, StatusEvent{
			InstanceID: instanceID,
			Status:     status,
			Message:    msg,
		})
	}
}

func (m *StatusMonitor) publishWithRetry(ctx context.Context, event StatusEvent) {
	maxAttempts := m.cfg.PublishMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	backoff := 10 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := m.publisher.Publish(ctx, event); err != nil {
			m.logger.Error("failed to publish status event",
				"error", err,
				"instanceID", event.InstanceID,
				"attempt", attempt,
			)
			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
			}
			continue
		}
		return
	}
}

// sweepInstances reconciles every instance ID known across all three indexers.
// It is called once after all caches have synced to publish the authoritative
// initial status for every managed database.
func sweepInstances(clusterIndexer, podIndexer, pvcIndexer cache.Indexer, debouncer *Debouncer) {
	seen := make(map[string]struct{})
	for _, id := range clusterIndexer.ListIndexFuncValues(instanceIDIndex) {
		seen[id] = struct{}{}
	}
	for _, id := range podIndexer.ListIndexFuncValues(instanceIDIndex) {
		seen[id] = struct{}{}
	}
	for _, id := range pvcIndexer.ListIndexFuncValues(instanceIDIndex) {
		seen[id] = struct{}{}
	}
	for instanceID := range seen {
		cluster := lookupClusterByIndex(clusterIndexer, instanceID)
		pods := lookupPodsByIndex(podIndexer, instanceID)
		pvcs := lookupPvcsByIndex(pvcIndexer, instanceID)
		reconcileAndSubmit(instanceID, cluster, pods, pvcs, debouncer)
	}
}

func lookupPodsByIndex(indexer cache.Indexer, instanceID string) []*corev1.Pod {
	items, err := indexer.ByIndex(instanceIDIndex, instanceID)
	if err != nil || len(items) == 0 {
		return nil
	}
	var pods []*corev1.Pod
	for _, podAny := range items {
		if pod, ok := podAny.(*corev1.Pod); ok {
			pods = append(pods, pod)
		}
	}
	return pods
}

func lookupPvcsByIndex(indexer cache.Indexer, instanceID string) []*corev1.PersistentVolumeClaim {
	items, err := indexer.ByIndex(instanceIDIndex, instanceID)
	if err != nil || len(items) == 0 {
		return nil
	}
	var pvcs []*corev1.PersistentVolumeClaim
	for _, pvcAny := range items {
		if pvc, ok := pvcAny.(*corev1.PersistentVolumeClaim); ok {
			pvcs = append(pvcs, pvc)
		}
	}
	return pvcs
}

func lookupClusterByIndex(indexer cache.Indexer, instanceID string) *cnpgv1.Cluster {
	items, err := indexer.ByIndex(instanceIDIndex, instanceID)
	if err != nil || len(items) == 0 {
		return nil
	}
	var cluster cnpgv1.Cluster
	clusterUnstructured, ok := items[0].(*unstructured.Unstructured)
	if !ok {
		return nil
	}

	err = runtime.DefaultUnstructuredConverter.FromUnstructured(clusterUnstructured.Object, &cluster)
	if err != nil {
		return nil
	}

	return &cluster
}
