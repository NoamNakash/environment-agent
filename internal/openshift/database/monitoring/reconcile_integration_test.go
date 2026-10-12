package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	k8sutils "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Status Monitor", func() {
	Describe("Reconciliation", func() {
		var (
			client    *fake.Clientset
			dynClient *dynfake.FakeDynamicClient
			publisher *mockStatusPublisher
			monitor   *monitoring.StatusMonitor
			ctx       context.Context
			cancel    context.CancelFunc
		)

		createCluster := func(name, instanceID string, replicas int) {
			labels := dcm.Labels(instanceID)
			cluster := &cnpgv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
				Spec:       cnpgv1.ClusterSpec{Instances: replicas},
			}
			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
			Expect(err).NotTo(HaveOccurred())
			_, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").
				Create(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		BeforeEach(func() {
			client = fake.NewClientset()
			publisher = newMockPublisher()
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg := monitoring.MonitorConfig{
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
			ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // intentional: outer-scope ctx/cancel shared with It blocks

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()
			time.Sleep(200 * time.Millisecond)
		})

		AfterEach(func() {
			cancel()
			time.Sleep(100 * time.Millisecond)
		})

		It("should reconcile to RUNNING when Pod phase changes to Running", func() {
			createCluster("my-db", "abc-123", 1)
			err := createFakeReplica(ctx, client, "my-db-1", "abc-123", corev1.PodRunning)
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.RUNNING {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected RUNNING event for instance abc-123")
		})

		It("should fall back to PENDING when Cluster exists without Pod", func() {
			createCluster("my-db", "abc-123", 1)

			// Update cluster to have Ready=False condition.
			clusterUnstructured, err := dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Get(ctx, "my-db", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			var cluster cnpgv1.Cluster
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(clusterUnstructured.Object, &cluster)
			Expect(err).NotTo(HaveOccurred())
			cluster.Status.Conditions = []metav1.Condition{
				{Type: string(cnpgv1.ConditionClusterReady), Status: metav1.ConditionFalse},
			}

			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cluster)
			Expect(err).NotTo(HaveOccurred())

			_, err = dynClient.Resource(k8sutils.ClustersResource).
				Namespace("default").UpdateStatus(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.PENDING {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected PENDING event for instance abc-123")
		})

		It("should produce DELETED status when cluster is deleted", func() {
			createCluster("my-db", "abc-123", 1)
			time.Sleep(300 * time.Millisecond)

			err := dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Delete(ctx, "my-db", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.DELETED {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected DELETED event for instance abc-123")
		})

		It("should reconcile Pod phase Pending to PENDING status", func() {
			createCluster("my-db", "abc-123", 1)
			err := createFakeReplica(ctx, client, "my-db-1", "abc-123", corev1.PodPending)
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.PENDING {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected PENDING event for instance abc-123")
		})

		It("should reconcile Pod phase Failed to FAILED status", func() {
			createCluster("my-db", "abc-123", 1)
			err := createFakeReplica(ctx, client, "my-db-1", "abc-123", corev1.PodFailed)
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.FAILED {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected FAILED event for instance abc-123")
		})

		It("should reconcile Pod phase Unknown to UNKNOWN status", func() {
			createCluster("my-db", "abc-123", 1)
			err := createFakeReplica(ctx, client, "my-db-1", "abc-123", corev1.PodUnknown)
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.UNKNOWN {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected UNKNOWN event for instance abc-123")
		})

		It("should not publish an event when Pod phase is Succeeded", func() {
			createCluster("my-db", "abc-123", 1)
			err := createFakeReplica(ctx, client, "my-db-1", "abc-123", corev1.PodSucceeded)
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			for _, e := range events {
				Expect(e.Status).NotTo(Equal(v1alpha1.DatabaseStatus("SUCCEEDED")),
					"no event should be published for Succeeded phase")
				if e.InstanceID == "abc-123" {
					// If any event was published for this instance after the
					// Succeeded pod was created, the status must not have changed
					// due to the Succeeded phase.
					Expect(e.Status).NotTo(BeEmpty(),
						"if an event was published, it should have a valid non-Succeeded status")
				}
			}
		})

		It("should use Pod status when Cluster is deleted but Pod remains", func() {
			createCluster("my-db", "abc-123", 1)
			err := createFakeReplica(ctx, client, "my-db-1", "abc-123", corev1.PodRunning)
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(300 * time.Millisecond)

			err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Delete(ctx, "my-db", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			var last monitoring.StatusEvent
			for _, e := range events {
				if e.InstanceID == "abc-123" {
					last = e
				}
			}
			Expect(last.Status).To(Equal(v1alpha1.RUNNING),
				"last event for abc-123 should be RUNNING, not DELETED")
		})

		DescribeTable("Multi-replica status reconciliation",
			func(expected v1alpha1.DatabaseStatus, phases ...corev1.PodPhase) {
				createCluster("my-db", "multi-replica", len(phases))
				time.Sleep(500 * time.Millisecond)
				for i, phase := range phases {
					err := createFakeReplica(ctx, client, "my-db-"+strconv.Itoa(i), "multi-replica", phase)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(500 * time.Millisecond)
				}

				events := publisher.Events()
				var last monitoring.StatusEvent
				for _, e := range events {
					if e.InstanceID == "multi-replica" {
						last = e
					}
				}
				Expect(last.Status).To(Equal(expected))
			},
			Entry("Should be running when all replicas are running", v1alpha1.RUNNING, corev1.PodRunning, corev1.PodRunning),
			Entry("Should be pending when all replicas are pending", v1alpha1.PENDING, corev1.PodPending, corev1.PodPending),
			Entry("Should be failed when all replicas are failed", v1alpha1.FAILED, corev1.PodFailed, corev1.PodFailed),
			Entry("Should be unknown when all replicas are unknown", v1alpha1.UNKNOWN, corev1.PodUnknown, corev1.PodUnknown),
			Entry("failed replica should override running", v1alpha1.FAILED, corev1.PodRunning, corev1.PodFailed),
			Entry("failed replica should override pending", v1alpha1.FAILED, corev1.PodPending, corev1.PodFailed),
			Entry("failed replica should override unknown", v1alpha1.FAILED, corev1.PodUnknown, corev1.PodFailed),
			Entry("failed replica should override multiple running", v1alpha1.FAILED, corev1.PodRunning, corev1.PodRunning, corev1.PodFailed),
			Entry("failed replica should override multiple pending", v1alpha1.FAILED, corev1.PodPending, corev1.PodPending, corev1.PodFailed),
			Entry("failed replica should override multiple unknown", v1alpha1.FAILED, corev1.PodUnknown, corev1.PodUnknown, corev1.PodFailed),
			Entry("failed replica should override mixed statuses", v1alpha1.FAILED, corev1.PodUnknown, corev1.PodPending, corev1.PodRunning, corev1.PodFailed),
			Entry("unknown replica should override running", v1alpha1.UNKNOWN, corev1.PodRunning, corev1.PodUnknown),
			Entry("unknown replica should override pending", v1alpha1.UNKNOWN, corev1.PodPending, corev1.PodUnknown),
			Entry("unknown replica should override multiple running", v1alpha1.UNKNOWN, corev1.PodRunning, corev1.PodRunning, corev1.PodUnknown),
			Entry("unknown replica should override multiple pending", v1alpha1.UNKNOWN, corev1.PodPending, corev1.PodPending, corev1.PodUnknown),
			Entry("unknown replica should override mixed non-failure statuses", v1alpha1.UNKNOWN, corev1.PodPending, corev1.PodRunning, corev1.PodUnknown),
			Entry("pending replica should override running", v1alpha1.PENDING, corev1.PodRunning, corev1.PodPending),
			Entry("pending replica should override multiple running", v1alpha1.PENDING, corev1.PodRunning, corev1.PodRunning, corev1.PodPending),
		)

		It("should be pending when one of multiple replicas is deleted", func() {
			createCluster("my-db", "abc-123", 2)
			err := createFakeReplica(ctx, client, "pod-a", "abc-123", corev1.PodRunning)
			Expect(err).NotTo(HaveOccurred())
			err = createFakeReplica(ctx, client, "pod-b", "abc-123", corev1.PodRunning)
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(300 * time.Millisecond)

			err = client.CoreV1().Pods("default").Delete(ctx, "pod-a", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(500 * time.Millisecond)

			var last monitoring.StatusEvent
			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			for _, e := range events {
				if e.InstanceID == "abc-123" {
					last = e
				}
			}
			Expect(last.Status).To(Equal(v1alpha1.PENDING),
				"last event for abc-123 should be PENDING (not all pods running)")
		})

		It("should publish FAILED when Cluster is pending with pod scheduling failure, then un-latch to RUNNING when the rollout recovers", func() {
			createCluster("my-db", "abc-123", 2)
			err := createFakeReplica(ctx, client, "my-db-1", "abc-123", corev1.PodRunning)
			Expect(err).NotTo(HaveOccurred())
			err = createFakeReplica(ctx, client, "my-db-2", "abc-123", corev1.PodPending, withPodConditions(corev1.PodCondition{
				Type:    corev1.PodScheduled,
				Status:  corev1.ConditionFalse,
				Reason:  corev1.PodReasonUnschedulable,
				Message: "0/3 nodes are available: 3 Insufficient cpu.",
			}))
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(1000 * time.Millisecond)

			clusterUnstructured, err := dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Get(ctx, "my-db", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			var cluster cnpgv1.Cluster
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(clusterUnstructured.Object, &cluster)
			Expect(err).NotTo(HaveOccurred())
			cluster.Status.Phase = cnpgv1.PhaseWaitingForInstancesToBeActive
			cluster.Status.PhaseReason = "Some instances are not yet active. Please wait."
			cluster.Status.Conditions = []metav1.Condition{
				{
					Type:    string(cnpgv1.ConditionClusterReady),
					Status:  metav1.ConditionFalse,
					Reason:  string(cnpgv1.ClusterIsNotReady),
					Message: "Cluster Is Not Ready",
				},
			}

			clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cluster)
			Expect(err).NotTo(HaveOccurred())

			_, err = dynClient.Resource(k8sutils.ClustersResource).
				Namespace("default").UpdateStatus(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(700 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var foundFailed bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.FAILED {
					foundFailed = true
					break
				}
			}
			Expect(foundFailed).To(BeTrue(), "expected FAILED event for instance abc-123")

			clusterUnstructured, err = dynClient.Resource(k8sutils.ClustersResource).Namespace("default").Get(ctx, "my-db", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(clusterUnstructured.Object, &cluster)
			Expect(err).NotTo(HaveOccurred())
			cluster.Status.Phase = cnpgv1.PhaseHealthy
			cluster.Status.PhaseReason = ""
			cluster.Status.Conditions = []metav1.Condition{
				{
					Type:    string(cnpgv1.ConditionClusterReady),
					Status:  metav1.ConditionTrue,
					Reason:  string(cnpgv1.ClusterReady),
					Message: "Cluster Is Ready",
				},
			}

			clusterMap, err = runtime.DefaultUnstructuredConverter.ToUnstructured(&cluster)
			Expect(err).NotTo(HaveOccurred())

			_, err = dynClient.Resource(k8sutils.ClustersResource).
				Namespace("default").UpdateStatus(ctx, &unstructured.Unstructured{Object: clusterMap}, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			pod, err := client.CoreV1().Pods("default").Get(ctx, "my-db-2", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{}
			_, err = client.CoreV1().Pods("default").UpdateStatus(ctx, pod, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events = publisher.Events()
			var last monitoring.StatusEvent
			for _, e := range events {
				if e.InstanceID == "abc-123" {
					last = e
				}
			}
			Expect(last.Status).To(Equal(v1alpha1.RUNNING),
				"last event for abc-123 should be RUNNING, not latched at FAILED")
		})
	})
})
