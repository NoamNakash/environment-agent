package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
			metav1.AddToGroupVersion(scheme, clustersGvr.GroupVersion())

			crds := map[schema.GroupVersionResource]string{
				clustersGvr: "ClusterList",
			}
			dynClient := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, crds)
			monitor = monitoring.NewStatusMonitor(client, cfg, publisher, logger)
		})

		It("should detect Deployment changes in the configured namespace (TC-I040)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			// Give informer time to start, then create a DCM-labeled Deployment.
			time.Sleep(200 * time.Millisecond)

			replicas := int32(1)
			_, err := client.AppsV1().Deployments("default").Create(ctx, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-deploy",
					Labels: dcm.Labels("abc-123"),
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{dcm.LabelInstanceID: "abc-123"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{dcm.LabelInstanceID: "abc-123"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
						},
					},
				},
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Wait for event processing.
			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty(), "expected at least one event from Deployment informer")
			Expect(events[len(events)-1].Status).To(Equal(v1alpha1.PENDING),
				"deploy-only (no pod) should produce PENDING status")
		})

		It("should detect Pod changes in the configured namespace (TC-I041)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			time.Sleep(200 * time.Millisecond)

			_, err := client.CoreV1().Pods("default").Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-pod",
					Labels: dcm.Labels("abc-123"),
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty(), "expected at least one event from Pod informer")
			Expect(events[len(events)-1].Status).To(Equal(v1alpha1.RUNNING),
				"pod with PodRunning phase should produce RUNNING status")
		})

		It("should only process resources with DCM labels (TC-I042)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			time.Sleep(200 * time.Millisecond)

			// Create a non-DCM deployment (no DCM labels).
			replicas := int32(1)
			_, err := client.AppsV1().Deployments("default").Create(ctx, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "non-dcm-deploy",
					Labels: map[string]string{"app": "other"},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "other"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": "other"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
						},
					},
				},
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).To(BeEmpty(), "non-DCM resources should not trigger events")
		})

		It("should enable index lookup by dcm-instance-id (TC-I043)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			time.Sleep(200 * time.Millisecond)

			// Create two DCM deployments with different instance IDs.
			for _, id := range []string{"inst-1", "inst-2"} {
				replicas := int32(1)
				_, err := client.AppsV1().Deployments("default").Create(ctx, &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "deploy-" + id,
						Labels: dcm.Labels(id),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: &replicas,
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{dcm.LabelInstanceID: id},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{dcm.LabelInstanceID: id},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
							},
						},
					},
				}, metav1.CreateOptions{})
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
			result, err := monitoring.InstanceIDIndexFunc(&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{dcm.LabelInstanceID: "abc-123"},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(ContainElement("abc-123"))

			// Verify ExtractInstanceID returns correct value.
			deploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{dcm.LabelInstanceID: "abc-123"},
				},
			}
			Expect(monitoring.ExtractInstanceID(deploy)).To(Equal("abc-123"))
		})
	})
})
