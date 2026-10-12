package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	k8sutils "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Status Monitor", func() {
	Describe("Lifecycle", func() {
		var (
			client    *fake.Clientset
			dynClient *dynfake.FakeDynamicClient
			publisher *mockStatusPublisher
			logger    *slog.Logger
			cfg       monitoring.MonitorConfig
		)

		BeforeEach(func() {
			client = fake.NewClientset()
			publisher = newMockPublisher()
			logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg = monitoring.MonitorConfig{
				Namespace:    "default",
				ProviderName: "k8s-sp",
				DebounceMs:   100,
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
			dynClient = dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, crds)
		})

		It("should not process events until Start is called", func() {
			monitor := monitoring.NewStatusMonitor(client, dynClient, cfg, publisher, logger)

			// Create a resource before Start is called.
			labels := dcm.Labels("abc-123")
			cluster := cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "pre-start",
					Labels: labels,
				},
				Spec: cnpgv1.ClusterSpec{
					Instances: 1,
				},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(context.Background(),
				&unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			// No events should be published yet because Start hasn't been called.
			time.Sleep(200 * time.Millisecond)
			Expect(publisher.Events()).To(BeEmpty(), "no events before Start")

			// Now start and verify events arrive.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			time.Sleep(500 * time.Millisecond)
			Expect(publisher.Events()).NotTo(BeEmpty(), "events should arrive after Start")
		})

		It("should stop watchers when context is cancelled", func() {
			monitor := monitoring.NewStatusMonitor(client, dynClient, cfg, publisher, logger)
			ctx, cancel := context.WithCancel(context.Background())

			started := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				close(started)
				_ = monitor.Start(ctx)
			}()
			<-started
			time.Sleep(200 * time.Millisecond)

			// Cancel the context — this should stop watchers.
			cancel()
			time.Sleep(200 * time.Millisecond)

			// Create a new resource after stop — it should NOT trigger events.
			eventsBefore := len(publisher.Events())

			labels := dcm.Labels("post-stop")
			cluster := cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "post-stop",
					Labels: labels,
				},
				Spec: cnpgv1.ClusterSpec{
					Instances: 1,
				},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(context.Background(),
				&unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(300 * time.Millisecond)
			Expect(publisher.Events()).To(HaveLen(eventsBefore),
				"no new events after context cancellation")
		})

		It("should not re-publish unchanged status on cache resync", func() {
			cfg.ResyncPeriod = 1 * time.Second
			monitor := monitoring.NewStatusMonitor(client, dynClient, cfg, publisher, logger)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Pre-create a resource so it's in the cache.
			labels := dcm.Labels("resync-test")
			cluster := cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "resync-cluster",
					Labels: labels,
				},
				Spec: cnpgv1.ClusterSpec{
					Instances: 1,
				},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(context.Background(),
				&unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			// Wait long enough for at least one resync to occur (resync period + buffer).
			time.Sleep(2 * time.Second)

			events := publisher.Events()
			// The initial sweep publishes exactly one event. Subsequent resyncs that
			// produce the same status must be suppressed by the debouncer dedup.
			Expect(events).To(HaveLen(1), "resync must not re-publish unchanged status")
		})

		It("should publish events for all existing resources on initial sync", func() {
			// Pre-create 3 DCM-managed resources.
			for i, id := range []string{"inst-1", "inst-2", "inst-3"} {
				labels := dcm.Labels(id)
				name := "cluster-" + string(rune('a'+i))
				cluster := cnpgv1.Cluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:   name,
						Labels: labels,
					},
					Spec: cnpgv1.ClusterSpec{
						Instances: 1,
					},
				}
				clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cluster)
				Expect(err).NotTo(HaveOccurred())
				_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(context.Background(),
					&unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
			}

			monitor := monitoring.NewStatusMonitor(client, dynClient, cfg, publisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			time.Sleep(1 * time.Second)

			events := publisher.Events()
			instanceIDs := make(map[string]bool)
			for _, e := range events {
				instanceIDs[e.InstanceID] = true
			}
			Expect(instanceIDs).To(HaveKey("inst-1"))
			Expect(instanceIDs).To(HaveKey("inst-2"))
			Expect(instanceIDs).To(HaveKey("inst-3"))
		})
	})
})
