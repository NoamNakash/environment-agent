package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	k8sutils "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Status Monitor", func() {
	Describe("Informer Setup", func() {
		var (
			client    *fake.Clientset
			dynClient *dynfake.FakeDynamicClient
			publisher *mockStatusPublisher
			monitor   *monitoring.StatusMonitor
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
			monitor = monitoring.NewStatusMonitor(client, dynClient, cfg, publisher, logger)
		})

		It("should detect Cluster changes in the configured namespace", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			// Give informer time to start, then create a DCM-labeled Cluster.
			time.Sleep(200 * time.Millisecond)

			cluster := &cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-cluster",
					Labels: dcm.Labels("abc-123"),
				},
				Spec: cnpgv1.ClusterSpec{
					Instances: 1,
				},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Wait for event processing.
			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty(), "expected at least one event from cluster informer")
			Expect(events[len(events)-1].Status).To(Equal(v1alpha1.PENDING),
				"cluster-only (no pod) should produce PENDING status")
		})

		It("should detect Pod changes in the configured namespace", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			time.Sleep(200 * time.Millisecond)
			err := createFakeReplica(ctx, client, "test-pod", "abc-123", v1.PodRunning)
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty(), "expected at least one event from Pod informer")
			Expect(events[len(events)-1].Status).To(Equal(v1alpha1.RUNNING),
				"pod with PodRunning phase should produce RUNNING status")
		})

		It("should only process resources with DCM labels", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			time.Sleep(200 * time.Millisecond)

			// Create a non-DCM cluster (no DCM labels).
			cluster := &cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "non-dcm-cluster",
					Labels: map[string]string{"app": "other"},
				},
				Spec: cnpgv1.ClusterSpec{
					Instances: 1,
				},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).To(BeEmpty(), "non-DCM resources should not trigger events")
		})

		It("should enable index lookup by dcm-instance-id", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			time.Sleep(200 * time.Millisecond)

			// Create two DCM clusters with different instance IDs.
			for _, id := range []string{"inst-1", "inst-2"} {
				cluster := &cnpgv1.Cluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "cluster-" + id,
						Labels: dcm.Labels(id),
					},
					Spec: cnpgv1.ClusterSpec{
						Instances: 1,
					},
				}
				clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
				Expect(err).NotTo(HaveOccurred())
				_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Create(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
			}

			time.Sleep(500 * time.Millisecond)

			// Verify events were published with the correct instance IDs.
			events := publisher.Events()
			instanceIDs := make(map[string]bool)
			for _, e := range events {
				instanceIDs[e.InstanceID] = true
			}
			Expect(instanceIDs).To(HaveKey("inst-1"))
			Expect(instanceIDs).To(HaveKey("inst-2"))

			// Also verify the indexer function directly.
			result, err := monitoring.InstanceIDIndexFunc(&cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{dcm.LabelInstanceID: "abc-123"},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(ContainElement("abc-123"))

			// Verify ExtractInstanceID returns correct value.
			cluster := &cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{dcm.LabelInstanceID: "abc-123"},
				},
			}
			Expect(monitoring.ExtractInstanceID(cluster)).To(Equal("abc-123"))
		})
	})
})
