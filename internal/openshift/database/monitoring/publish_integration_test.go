package monitoring_test

import (
	"encoding/json"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Status Monitor", func() {
	Describe("NATS Publishing", func() {
		It("should construct a valid CloudEvent with correct fields", func() {
			data, err := monitoring.NewStatusCloudEvent("dcm.database", "pgsql-sp", "abc-123", v1alpha1.RUNNING, "Pod is running")
			Expect(err).NotTo(HaveOccurred())
			Expect(data).NotTo(BeNil(), "expected non-nil CloudEvent JSON")

			var ce map[string]any
			Expect(json.Unmarshal(data, &ce)).To(Succeed())

			// CloudEvents v1.0 required attributes.
			Expect(ce).To(HaveKeyWithValue("specversion", "1.0"))
			Expect(ce).To(HaveKeyWithValue("source", "dcm/providers/pgsql-sp"))
			Expect(ce).To(HaveKeyWithValue("type", "dcm.status.database"))
			Expect(ce).To(HaveKeyWithValue("datacontenttype", "application/json"))
			Expect(ce).To(HaveKeyWithValue("subject", "dcm.database"))
			Expect(ce).To(HaveKey("id"))
			Expect(ce).To(HaveKey("time"))

			// Data payload.
			ceData, ok := ce["data"].(map[string]any)
			Expect(ok).To(BeTrue(), "data field should be a JSON object")
			Expect(ceData).To(HaveKeyWithValue("id", "abc-123"))
			Expect(ceData).To(HaveKeyWithValue("status", "RUNNING"))
		})

		It("should include failure reason in FAILED event message", func() {
			data, err := monitoring.NewStatusCloudEvent("dcm.database", "pgsql-sp", "abc-123", v1alpha1.FAILED, "CrashLoopBackOff")
			Expect(err).NotTo(HaveOccurred())
			Expect(data).NotTo(BeNil(), "expected non-nil CloudEvent JSON")

			var ce map[string]any
			Expect(json.Unmarshal(data, &ce)).To(Succeed())

			ceData, ok := ce["data"].(map[string]any)
			Expect(ok).To(BeTrue(), "data field should be a JSON object")
			Expect(ceData).To(HaveKeyWithValue("id", "abc-123"))
			Expect(ceData).To(HaveKeyWithValue("status", "FAILED"))
			Expect(ceData["message"]).To(ContainSubstring("CrashLoopBackOff"))
		})
	})
})
