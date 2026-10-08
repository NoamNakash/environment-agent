package monitoring

import (
	"fmt"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	k8sutil "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	corev1 "k8s.io/api/core/v1"
)

// progressDeadlineExceededReason is the Deployment condition Reason set by
// the controller once a rollout exceeds spec.progressDeadlineSeconds.
const progressDeadlineExceededReason = "ProgressDeadlineExceeded"

// ReconcileStatus derives the DCM container status from the current state of
// a Deployment and its associated Pod. It returns the status, a human-readable
// message, and whether the result should be published.
func ReconcileStatus(cluster *cnpgv1.Cluster, pods []*corev1.Pod, pvcs []*corev1.PersistentVolumeClaim) (v1alpha1.DatabaseStatus, string, bool) {
	if cluster == nil && len(pods) == 0 && len(pvcs) == 0 {
		return v1alpha1.DELETED, "resource no longer exists", true
	}

	// Stuck-rollout detection takes precedence over pod-phase mapping and is
	// evaluated regardless of Pod existence.
	if cluster != nil {
		if status, msg, ok := stuckRolloutStatus(cluster, pods); ok {
			return status, msg, true
		}
	}

	if len(pods) != 0 {
		var resolvedPods []corev1.Pod
		var resolvedPvcs []corev1.PersistentVolumeClaim

		for _, pod := range pods {
			resolvedPods = append(resolvedPods, *pod)
		}
		for _, pvc := range pvcs {
			resolvedPvcs = append(resolvedPvcs, *pvc)
		}
		replicas, err := k8sutil.MatchReplicas(resolvedPods, resolvedPvcs)
		if err != nil {
			return v1alpha1.PENDING, cluster.Status.PhaseReason, true
		}

		msg := ""
		var status v1alpha1.DatabaseStatus
		for _, replica := range replicas {
			status, ok := k8sutil.MapReplicaStatus(replica)
			if !ok {
				return "", "", false
			}

			if status == v1alpha1.FAILED {
				if reason := extractPodFailureReason(replica.Pod); reason != "" {
					msg = reason
				}
			}
		}
		if msg == "" {
			msg = cluster.Status.Phase
		}

		return status, msg, true

	}

	// Cluster-only path (pod == nil).
	if cluster.Spec.Instances != 0 {
		return v1alpha1.FAILED, "cluster scaled to zero", true
	}
	if cluster.Status.Phase == cnpgv1.PhaseWaitingForInstancesToBeActive ||
		cluster.Status.Phase == cnpgv1.PhaseCreatingReplica {
		return v1alpha1.PENDING, cluster.Status.Phase, true
	}
	return v1alpha1.PENDING, "waiting for pods", true
}

// stuckRolloutStatus reports FAILED when the Deployment controller has given
// up on the rollout progressing within spec.progressDeadlineSeconds.
func stuckRolloutStatus(cluster *cnpgv1.Cluster, pods []*corev1.Pod) (v1alpha1.DatabaseStatus, string, bool) {
	if cluster.Status.Phase == cnpgv1.PhaseWaitingForInstancesToBeActive || cluster.Status.Phase == cnpgv1.PhaseFailOver {
		var reason string
		for _, pod := range pods {
			if extractPodFailureReason(pod) != "" {
				reason = extractPodFailureReason(pod)
			}
		}
		msg := cluster.Status.PhaseReason
		if reason != "" {
			msg = fmt.Sprintf("%s (%s)", cluster.Status.PhaseReason, reason)
		}
		return v1alpha1.FAILED, msg, true

	}

	return "", "", false
}

// extractPodFailureReason attempts to find a specific failure reason from
// pod container statuses (e.g., CrashLoopBackOff).
func extractPodFailureReason(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
			return cs.State.Waiting.Reason
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason
		}
	}
	return ""
}
