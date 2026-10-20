package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	k8sutils "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var _ = Describe("Status Monitor", func() {
	Describe("Resilience", func() {
		It("should reconnect watchers after API server interruption", func() {
			client := fake.NewClientset()
			publisher := newMockPublisher()
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg := monitoring.MonitorConfig{
				Namespace:    "default",
				ProviderName: "pgsql-sp",
				DebounceMs:   50,
				ResyncPeriod: 1 * time.Second,
			}

			// the gvk object registers a specific "Kind", in this case, the single cluster resource
			gvk := schema.GroupVersionKind{
				Group:   "postgresql.cnpg.io",
				Version: "v1",
				Kind:    "Cluster",
			}

			// this gvk object registers a list of cluster resources as a single resource to enable
			// usage of the "List" function
			gvkList := schema.GroupVersionKind{
				Group:   "postgresql.cnpg.io",
				Version: "v1",
				Kind:    "ClusterList",
			}

			scheme := runtime.NewScheme()

			scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(gvkList, &unstructured.UnstructuredList{})
			metav1.AddToGroupVersion(scheme, k8sutils.ClustersResource.GroupVersion())

			crds := map[schema.GroupVersionResource]string{
				k8sutils.ClustersResource: "ClusterList",
			}
			dynClient := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, crds)
			// Set up reactor BEFORE starting the monitor to avoid racing with
			// the informer's concurrent use of the fake client.
			var failing atomic.Bool
			var callCount atomic.Int32
			dynClient.PrependReactor("list", "clusters", func(_ k8stesting.Action) (bool, runtime.Object, error) {
				if !failing.Load() {
					return false, nil, nil
				}
				n := callCount.Add(1)
				if n <= 2 {
					return true, nil, errors.NewServiceUnavailable("API server unavilable")
				}
				return false, nil, nil
			})

			monitor := monitoring.NewStatusMonitor(client, dynClient, cfg, publisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()
			time.Sleep(200 * time.Millisecond)

			// Simulate API server interruption.
			failing.Store(true)

			// Wait for reconnection attempt.
			time.Sleep(2 * time.Second)

			// Create a resource after "reconnection".
			labels := dcm.Labels("reconnect-test")
			cluster := &cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "after-reconnect",
					Labels: labels,
				},
				Spec: cnpgv1.ClusterSpec{
					Instances: 1,
				},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "reconnect-test" {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "events should resume after reconnection")
		})

		It("should honor PublishMaxAttempts when set in MonitorConfig", func() {
			// Use a publisher that always fails so we can count exactly how many
			// attempts are made. With PublishMaxAttempts=2, the monitor must stop
			// after exactly 2 attempts — not the hardcoded 5.
			failPublisher := &retryTrackingPublisher{failAlways: true}
			client := fake.NewClientset()
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg := monitoring.MonitorConfig{
				Namespace:          "default",
				ProviderName:       "pgsql-sp",
				DebounceMs:         50,
				ResyncPeriod:       1 * time.Second,
				PublishMaxAttempts: 2,
			}

			gvk := schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}
			gvkList := schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "ClusterList"}
			scheme := runtime.NewScheme()
			scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(gvkList, &unstructured.UnstructuredList{})
			metav1.AddToGroupVersion(scheme, k8sutils.ClustersResource.GroupVersion())
			crds := map[schema.GroupVersionResource]string{k8sutils.ClustersResource: "ClusterList"}
			dynClient := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, crds)

			monitor := monitoring.NewStatusMonitor(client, dynClient, cfg, failPublisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()
			time.Sleep(200 * time.Millisecond)

			labels := dcm.Labels("max-attempts-test")
			cluster := &cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "max-attempts-cluster", Labels: labels},
				Spec:       cnpgv1.ClusterSpec{Instances: 1},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Wait long enough for the first publishing round (2 attempts with 10ms
			// backoff ≈ 10ms), but less than one resync period (1s) so we see only
			// the first round. A failed publish is no longer recorded as delivered,
			// so subsequent resyncs will re-queue the event — but they haven't fired yet.
			time.Sleep(300 * time.Millisecond)

			// With PublishMaxAttempts=2, exactly 2 attempts should be made — not 5.
			Expect(failPublisher.attempts.Load()).To(Equal(int32(2)),
				"publishWithRetry must respect PublishMaxAttempts from config")
		})

		It("should retry publishing with exponential backoff on transient failure", func() {
			failPublisher := &retryTrackingPublisher{}
			client := fake.NewClientset()
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg := monitoring.MonitorConfig{
				Namespace:    "default",
				ProviderName: "pgsql-sp",
				DebounceMs:   50,
				ResyncPeriod: 1 * time.Second,
			}
			// the gvk object registers a specific "Kind", in this case, the single cluster resource
			gvk := schema.GroupVersionKind{
				Group:   "postgresql.cnpg.io",
				Version: "v1",
				Kind:    "Cluster",
			}

			// this gvk object registers a list of cluster resources as a single resource to enable
			// usage of the "List" function
			gvkList := schema.GroupVersionKind{
				Group:   "postgresql.cnpg.io",
				Version: "v1",
				Kind:    "ClusterList",
			}

			scheme := runtime.NewScheme()

			scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(gvkList, &unstructured.UnstructuredList{})
			metav1.AddToGroupVersion(scheme, k8sutils.ClustersResource.GroupVersion())

			crds := map[schema.GroupVersionResource]string{
				k8sutils.ClustersResource: "ClusterList",
			}
			dynClient := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, crds)

			monitor := monitoring.NewStatusMonitor(client, dynClient, cfg, failPublisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()
			time.Sleep(200 * time.Millisecond)

			// Create a resource to trigger a publish.
			labels := dcm.Labels("retry-test")
			cluster := &cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "retry-cluster",
					Labels: labels,
				},
				Spec: cnpgv1.ClusterSpec{
					Instances: 1,
				},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Wait for retries.
			time.Sleep(2 * time.Second)

			// Verify multiple attempts were made.
			Expect(failPublisher.attempts.Load()).To(BeNumerically(">=", 2),
				"publisher should retry on transient failures")
		})

		It("should continue serving when NATS is unavailable", func() {
			logBuf := &safeBuffer{}
			failPublisher := &retryTrackingPublisher{failAlways: true}
			client := fake.NewClientset()
			logger := slog.New(slog.NewJSONHandler(logBuf, nil))
			cfg := monitoring.MonitorConfig{
				Namespace:    "default",
				ProviderName: "pgsql-sp",
				DebounceMs:   50,
				ResyncPeriod: 1 * time.Second,
			}
			// the gvk object registers a specific "Kind", in this case, the single cluster resource
			gvk := schema.GroupVersionKind{
				Group:   "postgresql.cnpg.io",
				Version: "v1",
				Kind:    "Cluster",
			}

			// this gvk object registers a list of cluster resources as a single resource to enable
			// usage of the "List" function
			gvkList := schema.GroupVersionKind{
				Group:   "postgresql.cnpg.io",
				Version: "v1",
				Kind:    "ClusterList",
			}

			scheme := runtime.NewScheme()

			scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(gvkList, &unstructured.UnstructuredList{})
			metav1.AddToGroupVersion(scheme, k8sutils.ClustersResource.GroupVersion())

			crds := map[schema.GroupVersionResource]string{
				k8sutils.ClustersResource: "ClusterList",
			}
			dynClient := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, crds)

			monitor := monitoring.NewStatusMonitor(client, dynClient, cfg, failPublisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()
			time.Sleep(200 * time.Millisecond)

			// Create a resource that will trigger a publish (which will fail).
			labels := dcm.Labels("nats-down")
			cluster := &cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "nats-down-cluster",
					Labels: labels,
				},
				Spec: cnpgv1.ClusterSpec{
					Instances: 1,
				},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(1 * time.Second)

			// Monitor should still be operational (Start did not return with error / panic).
			// The failure should be logged.
			Expect(logBuf.String()).To(ContainSubstring("error"),
				"NATS failure should be logged")
		})
	})
})

// safeBuffer is a thread-safe wrapper around bytes.Buffer for use as an
// io.Writer in tests where concurrent writes and reads are expected.
type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// retryTrackingPublisher is a mock publisher that fails N times then succeeds,
// tracking attempt counts for retry verification.
type retryTrackingPublisher struct {
	attempts   atomic.Int32
	failAlways bool
}

func (p *retryTrackingPublisher) Publish(_ context.Context, _ monitoring.StatusEvent) error {
	n := p.attempts.Add(1)
	if p.failAlways || n <= 3 {
		return context.DeadlineExceeded // Simulate transient failure.
	}
	return nil
}

func (p *retryTrackingPublisher) Close() error {
	return nil
}
