package analysis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/argoproj/argo-rollouts/metric"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/record"
)

// providerTypeMetadataKey records the type (name) of the metric provider which produced a
// MetricResult. It is stored in MetricResult.Metadata so abort events can identify the provider
// that served the triggering measurement.
const providerTypeMetadataKey = "providerType"

// measurementIDMetadataKey is an optional, provider-assigned stable identifier for a measurement
// (e.g. a query run ID). When present, it is used to deduplicate retried/delayed results.
const measurementIDMetadataKey = "measurementID"

// retryableErrorMarkers are substrings commonly surfaced by metric providers when a measurement
// failed for a transient reason (deadline, connection reset, DNS hiccup, ...). Providers only
// communicate failures through the Measurement.Message, so the markers are matched case-insensitively.
var retryableErrorMarkers = []string{
	"timeout",
	"timed out",
	"deadline exceeded",
	"context deadline exceeded",
	"connection refused",
	"connection reset",
	"connection aborted",
	"temporarily unavailable",
	"server is busy",
	"too many requests",
	"no such host",
	"network is unreachable",
	"eof",
	"transport error",
	"temporary failure",
	"i/o timeout",
}

// IsRetryableMeasurementError reports whether a completed Error measurement failed for a transient
// reason (provider timeout, network/connectivity problem, rate limiting, ...). Such measurements
// are retried and only count towards consecutiveErrorLimit; they are never treated as a failed
// assessment of the metric. A malformed result or an explicit non-retryable error returns false so
// that unknown results are never silently treated as success either.
func IsRetryableMeasurementError(measurement v1alpha1.Measurement) bool {
	if measurement.Phase != v1alpha1.AnalysisPhaseError {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(measurement.Message))
	if message == "" {
		return false
	}
	for _, marker := range retryableErrorMarkers {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

// IsRetryableError reports whether err is a transient error from a metric provider call. It
// recognizes context deadlines/cancellation and the standard net timeout/temporary errors.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	return IsRetryableMeasurementError(v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseError, Message: err.Error()})
}

// measurementIdentity returns a stable identity for a measurement used to recognize duplicate
// results delivered by a retrying or delayed provider. A provider-assigned ID wins when available;
// otherwise the identity is derived from the measurement window (StartedAt/FinishedAt) together
// with its phase and value. Empty timestamps produce an empty identity, meaning callers must fall
// back to order-based handling for measurements without temporal information.
func measurementIdentity(measurement v1alpha1.Measurement) string {
	if id, ok := measurement.Metadata[measurementIDMetadataKey]; ok && id != "" {
		return "id:" + id
	}
	if measurement.StartedAt == nil || measurement.FinishedAt == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("window:")
	sb.WriteString(measurement.StartedAt.UTC().Format(metav1.RFC3339Micro))
	sb.WriteString("|")
	sb.WriteString(measurement.FinishedAt.UTC().Format(metav1.RFC3339Micro))
	sb.WriteString("|")
	sb.WriteString(string(measurement.Phase))
	sb.WriteString("|")
	sb.WriteString(measurement.Value)
	return sb.String()
}

// isDuplicateMeasurement reports whether measurement has already been recorded for the metric.
// Only completed measurements are considered, since an in-progress placeholder in the history is
// the slot which a resumed measurement is allowed to replace.
func isDuplicateMeasurement(history []v1alpha1.Measurement, measurement v1alpha1.Measurement) bool {
	if !measurement.Phase.Completed() {
		return false
	}
	identity := measurementIdentity(measurement)
	if identity == "" {
		return false
	}
	for _, existing := range history {
		if !existing.Phase.Completed() {
			continue
		}
		if measurementIdentity(existing) == identity {
			return true
		}
	}
	return false
}

// isStaleMeasurement reports whether a completed measurement arrived out of order and describes an
// older observation than the latest completed measurement on record. First-write-wins ordering
// ensures delayed results never overwrite a newer measurement. In-progress placeholders and runs
// without any completed measurement are never considered stale.
func isStaleMeasurement(history []v1alpha1.Measurement, measurement v1alpha1.Measurement) bool {
	if !measurement.Phase.Completed() || measurement.FinishedAt == nil {
		return false
	}
	for i := len(history) - 1; i >= 0; i-- {
		existing := history[i]
		if !existing.Phase.Completed() || existing.FinishedAt == nil {
			continue
		}
		return measurement.FinishedAt.Time.Before(existing.FinishedAt.Time)
	}
	return false
}

// shouldDiscardMeasurement reports whether an incoming measurement must not be applied to the
// metric result because it is a duplicate of an already accepted measurement or an out-of-order
// (stale) result. When it returns true the controller keeps the previously accepted measurement and
// its counters untouched.
func shouldDiscardMeasurement(history []v1alpha1.Measurement, measurement v1alpha1.Measurement) bool {
	return isDuplicateMeasurement(history, measurement) || isStaleMeasurement(history, measurement)
}

// ConsecutiveFailures counts the number of consecutive Failed measurements at the tail of the
// metric's measurement history. A Successful measurement resets the count to zero; Error and
// Inconclusive measurements interrupt a failure streak but are not failures themselves. It is
// computed from the retained history so no API changes are required.
func ConsecutiveFailures(result v1alpha1.MetricResult) int32 {
	var consecutive int32
	for i := len(result.Measurements) - 1; i >= 0; i-- {
		switch result.Measurements[i].Phase {
		case v1alpha1.AnalysisPhaseFailed:
			consecutive++
		case v1alpha1.AnalysisPhaseSuccessful:
			return consecutive
		default:
			// Errors (including retried transient errors) and inconclusive results break the
			// streak without counting as failures.
			if result.Measurements[i].Phase.Completed() {
				return consecutive
			}
		}
	}
	return consecutive
}

// metricProviderName derives the configured provider name from a MetricProvider spec. It is used
// when no provider instance could be constructed (e.g. provider unreachable) so errors can still
// name the intended provider.
func metricProviderName(spec v1alpha1.MetricProvider) string {
	switch {
	case spec.Prometheus != nil:
		return "Prometheus"
	case spec.Kayenta != nil:
		return "Kayenta"
	case spec.Web != nil:
		return "Web"
	case spec.Datadog != nil:
		return "Datadog"
	case spec.Wavefront != nil:
		return "Wavefront"
	case spec.NewRelic != nil:
		return "NewRelic"
	case spec.Job != nil:
		return "Job"
	case spec.CloudWatch != nil:
		return "CloudWatch"
	case spec.Graphite != nil:
		return "Graphite"
	case spec.Influxdb != nil:
		return "Influxdb"
	case spec.SkyWalking != nil:
		return "SkyWalking"
	case len(spec.Plugin) > 0:
		return "Plugin"
	default:
		return "Unknown"
	}
}

// providerTypeName returns the provider Type() when available for inclusion in events and
// metadata. A construction error leaves the provider nil, and a provider which does not expect
// Type() to be called (e.g. a test mock) is recovered as an empty string; callers fall back to
// the metric name.
func providerTypeName(provider metric.Provider) (name string) {
	if provider == nil {
		return ""
	}
	defer func() {
		if r := recover(); r != nil {
			name = ""
		}
	}()
	return provider.Type()
}

// recordMetricAbortEvent emits a warning event when a metric reaches the Failed or Error terminal
// phase. The event message names the triggering measurement (phase, value and finished time), the
// metric provider which served it, and the failure counts that caused the threshold to be reached,
// so rollout aborts can be traced back to the exact observation.
func (c *Controller) recordMetricAbortEvent(run *v1alpha1.AnalysisRun, metric v1alpha1.Metric, result v1alpha1.MetricResult, phase v1alpha1.AnalysisPhase, eventType string) {
	providerName := result.Metadata[providerTypeMetadataKey]
	if providerName == "" {
		providerName = metric.Name
	}
	triggerDesc := "no measurement recorded"
	if len(result.Measurements) > 0 {
		trigger := result.Measurements[len(result.Measurements)-1]
		finishedAt := "<unknown>"
		if trigger.FinishedAt != nil {
			finishedAt = trigger.FinishedAt.UTC().Format(metav1.RFC3339Micro)
		}
		triggerDesc = fmt.Sprintf("measurement phase=%s value=%q finishedAt=%s", trigger.Phase, trigger.Value, finishedAt)
		if trigger.Message != "" {
			triggerDesc += fmt.Sprintf(" message=%q", trigger.Message)
		}
	}
	message := fmt.Sprintf(
		"Metric '%s' assessed %s. Provider: %s. Triggering %s. consecutiveFailed=%d failed=%d consecutiveError=%d error=%d",
		metric.Name, phase, providerName, triggerDesc,
		ConsecutiveFailures(result), result.Failed, result.ConsecutiveError, result.Error,
	)
	c.recorder.Eventf(run, record.EventOptions{EventType: eventType, EventReason: "Metric" + string(phase)}, message)
}
