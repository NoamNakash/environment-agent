package monitoring_test

import (
	"sync"
	"sync/atomic"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Status Monitor", func() {
	Describe("Debounce", func() {
		It("should coalesce rapid events within the debounce window", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})
			defer debouncer.Stop()

			// Submit 3 rapid changes within the debounce window.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.PENDING,
			})
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.FAILED,
			})

			// Wait for debounce window to elapse.
			time.Sleep(300 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(1), "only the last event should be published")
			Expect(published[0].Status).To(Equal(v1alpha1.FAILED))
		})

		It("should debounce events independently per instance ID", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})
			defer debouncer.Stop()

			// Rapid changes for instance A — should coalesce to last event only.
			debouncer.Submit("instance-a", monitoring.StatusEvent{
				InstanceID: "instance-a",
				Status:     v1alpha1.PENDING,
			})
			debouncer.Submit("instance-a", monitoring.StatusEvent{
				InstanceID: "instance-a",
				Status:     v1alpha1.RUNNING,
			})

			// Single event for instance B — should publish independently.
			debouncer.Submit("instance-b", monitoring.StatusEvent{
				InstanceID: "instance-b",
				Status:     v1alpha1.FAILED,
			})

			// Wait for debounce window to elapse.
			time.Sleep(300 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(2), "each instance should publish independently")

			statuses := map[string]v1alpha1.DatabaseStatus{}
			for _, e := range published {
				statuses[e.InstanceID] = e.Status
			}
			Expect(statuses).To(HaveKeyWithValue("instance-a", v1alpha1.RUNNING))
			Expect(statuses).To(HaveKeyWithValue("instance-b", v1alpha1.FAILED))
		})

		It("should publish events separately when separated by full window gap", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})
			defer debouncer.Stop()

			// First event.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})

			// Wait for debounce window to fully elapse.
			time.Sleep(200 * time.Millisecond)

			// Second event after the window.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.FAILED,
			})

			// Wait for second window to elapse.
			time.Sleep(200 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(2), "events separated by window gap should publish separately")
			Expect(published[0].Status).To(Equal(v1alpha1.RUNNING))
			Expect(published[1].Status).To(Equal(v1alpha1.FAILED))
		})

		It("should wait for in-flight publish before Stop returns", func() {
			publishStarted := make(chan struct{})
			publishComplete := make(chan struct{})

			debouncer := monitoring.NewDebouncer(10*time.Millisecond, func(_ monitoring.StatusEvent) {
				close(publishStarted)
				<-publishComplete
			})

			// Submit an event and wait for the callback to start executing.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})

			Eventually(publishStarted).Should(BeClosed())

			// Call Stop concurrently — it should block while publish is in flight.
			stopDone := make(chan struct{})
			go func() {
				debouncer.Stop()
				close(stopDone)
			}()

			// Stop should NOT have returned yet (publish still blocked).
			Consistently(stopDone, 50*time.Millisecond).ShouldNot(BeClosed())

			// Unblock the publish callback.
			close(publishComplete)

			// Now Stop should return promptly.
			Eventually(stopDone, 100*time.Millisecond).Should(BeClosed())

			// After Stop, no further publishes should occur.
			var postStopPublished atomic.Int32
			debouncer2 := monitoring.NewDebouncer(10*time.Millisecond, func(_ monitoring.StatusEvent) {
				postStopPublished.Add(1)
			})
			debouncer2.Stop()
			debouncer2.Submit("xyz-789", monitoring.StatusEvent{
				InstanceID: "xyz-789",
				Status:     v1alpha1.PENDING,
			})
			time.Sleep(50 * time.Millisecond)
			Expect(postStopPublished.Load()).To(Equal(int32(0)), "no publish should occur after Stop")
		})
	})
})
