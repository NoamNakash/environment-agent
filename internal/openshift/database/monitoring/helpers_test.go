package monitoring_test

import (
	"context"
	"sync"

	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	k8sutils "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Compile-time assertion: NATSPublisher implements StatusPublisher (TC-U072).
var _ monitoring.StatusPublisher = (*monitoring.NATSPublisher)(nil)

// mockStatusPublisher records all published events for test assertions.
type mockStatusPublisher struct {
	mu     sync.Mutex
	events []monitoring.StatusEvent
	err    error
}

func newMockPublisher() *mockStatusPublisher {
	return &mockStatusPublisher{}
}

func (m *mockStatusPublisher) Publish(_ context.Context, event monitoring.StatusEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.events = append(m.events, event)
	return nil
}

func (m *mockStatusPublisher) Close() error {
	return nil
}

func (m *mockStatusPublisher) Events() []monitoring.StatusEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]monitoring.StatusEvent, len(m.events))
	copy(cp, m.events)
	return cp
}

type replicaOption func(*corev1.Pod, *corev1.PersistentVolumeClaim)

func withPodConditions(conditions ...corev1.PodCondition) replicaOption {
	return func(pod *corev1.Pod, _ *corev1.PersistentVolumeClaim) {
		pod.Status.Conditions = conditions
	}
}

func createFakeReplica(ctx context.Context, client kubernetes.Interface, name, instanceId string, podPhase corev1.PodPhase, opts ...replicaOption) error {
	pgdata := "pgdata"
	pvcLabels := dcm.Labels(instanceId)

	pvcLabels[k8sutils.InstanceLabel] = name
	pvcLabels["cnpg.io/pvcRole"] = pgdata

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: pvcLabels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: new("standard"),
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	podLabels := dcm.Labels(instanceId)
	podLabels["cnpg.io/podRole"] = "instance"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: podLabels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "postgres",
					Image: "ghcr.io/cloudnative-pg/postgresql:18.4-system-trixie",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      pgdata,
							MountPath: "/var/lib/postgresql/data",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: pgdata,
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: name,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: podPhase,
		},
	}

	for _, opt := range opts {
		opt(pod, pvc)
	}

	_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	_, err = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	return err
}
