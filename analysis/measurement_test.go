package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8srecord "k8s.io/client-go/tools/record"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
)

func ptrTime(t metav1.Time) *metav1.Time {
	return &t
}

func nowPtr() *metav1.Time {
	now := timeutil.MetaNow()
	return &now
}

func int32Ptr(v int32) *intstr.IntOrString {
	ios := intstr.FromInt(int(v))
	return &ios
}

type recordedEvent struct {
	Reason  string
	Message string
}

// eventRecorderEvents drains the fake recorder's event channel. Each channel entry is formatted
// as "EventType EventReason EventMessage" by the Kubernetes fake recorder.
func (f *fixture) eventRecorderEvents() []recordedEvent {
	fakeK8s, ok := f.fakeRecorder.K8sRecorder().(*k8srecord.FakeRecorder)
	if !ok {
		return nil
	}
	var out []recordedEvent
	for {
		select {
		case entry := <-fakeK8s.Events:
			parts := strings.SplitN(entry, " ", 3)
			ev := recordedEvent{}
			if len(parts) >= 2 {
				ev.Reason = parts[1]
			}
			if len(parts) >= 3 {
				ev.Message = parts[2]
			}
			out = append(out, ev)
		default:
			return out
		}
	}
}

// advance moves the fixture's frozen clock forward so the next reconciliation considers the
// metric interval/error-retry interval elapsed and schedules a new measurement.
func (f *fixture) advance(d time.Duration) {
	f.now = f.now.Add(d)
}

func measurementAt(phase v1alpha1.AnalysisPhase, value string, finishedAt time.Time) v1alpha1.Measurement {
	start := metav1.NewTime(finishedAt.Add(-time.Second))
	end := metav1.NewTime(finishedAt)
	return v1alpha1.Measurement{
		Phase:      phase,
		Value:      value,
		StartedAt:  &start,
		FinishedAt: &end,
	}
}

func TestMeasurementIdentity(t *testing.T) {
	now := time.Now().UTC()
	m1 := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "1", now)
	m2 := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "1", now)
	m3 := measurementAt(v1alpha1.AnalysisPhaseFailed, "1", now)
	m4 := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "2", now)

	assert.Equal(t, measurementIdentity(m1), measurementIdentity(m2))
	assert.NotEqual(t, measurementIdentity(m1), measurementIdentity(m3))
	assert.NotEqual(t, measurementIdentity(m1), measurementIdentity(m4))

	// provider-assigned identity takes precedence and can match even if timestamps differ
	m5 := v1alpha1.Measurement{
		Phase:    v1alpha1.AnalysisPhaseSuccessful,
		Metadata: map[string]string{measurementIDMetadataKey: "run-123"},
	}
	m6 := v1alpha1.Measurement{
		Phase:      v1alpha1.AnalysisPhaseSuccessful,
		StartedAt:  m1.StartedAt,
		FinishedAt: m1.FinishedAt,
		Metadata:   map[string]string{measurementIDMetadataKey: "run-123"},
	}
	assert.Equal(t, measurementIdentity(m5), measurementIdentity(m6))

	// missing timestamps => no window identity
	assert.Empty(t, measurementIdentity(v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseSuccessful}))
}

func TestIsDuplicateMeasurement(t *testing.T) {
	now := time.Now().UTC()
	history := []v1alpha1.Measurement{
		measurementAt(v1alpha1.AnalysisPhaseSuccessful, "1", now.Add(-2*time.Minute)),
		measurementAt(v1alpha1.AnalysisPhaseFailed, "2", now.Add(-time.Minute)),
	}

	t.Run("exact duplicate is detected", func(t *testing.T) {
		dup := measurementAt(v1alpha1.AnalysisPhaseFailed, "2", now.Add(-time.Minute))
		assert.True(t, isDuplicateMeasurement(history, dup))
	})

	t.Run("distinct measurement is not duplicate", func(t *testing.T) {
		next := measurementAt(v1alpha1.AnalysisPhaseFailed, "3", now)
		assert.False(t, isDuplicateMeasurement(history, next))
	})

	t.Run("incoming in-progress measurement never duplicates", func(t *testing.T) {
		inflight := v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseRunning}
		assert.False(t, isDuplicateMeasurement(history, inflight))
	})

	t.Run("history in-progress placeholder is ignored", func(t *testing.T) {
		withInflight := append([]v1alpha1.Measurement{{Phase: v1alpha1.AnalysisPhaseRunning}}, history...)
		dup := measurementAt(v1alpha1.AnalysisPhaseFailed, "2", now.Add(-time.Minute))
		assert.True(t, isDuplicateMeasurement(withInflight, dup))
	})
}

func TestIsStaleMeasurement(t *testing.T) {
	now := time.Now().UTC()
	history := []v1alpha1.Measurement{
		measurementAt(v1alpha1.AnalysisPhaseSuccessful, "1", now.Add(-2*time.Minute)),
		measurementAt(v1alpha1.AnalysisPhaseFailed, "2", now.Add(-time.Minute)),
	}

	t.Run("older out-of-order result is stale", func(t *testing.T) {
		late := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "9", now.Add(-90*time.Second))
		assert.True(t, isStaleMeasurement(history, late))
	})

	t.Run("newer result is not stale", func(t *testing.T) {
		fresh := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "3", now)
		assert.False(t, isStaleMeasurement(history, fresh))
	})

	t.Run("result without finished time is not stale", func(t *testing.T) {
		assert.False(t, isStaleMeasurement(history, v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseError}))
	})

	t.Run("empty history is never stale", func(t *testing.T) {
		fresh := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "3", now)
		assert.False(t, isStaleMeasurement(nil, fresh))
	})
}

func TestShouldDiscardMeasurement(t *testing.T) {
	now := time.Now().UTC()
	history := []v1alpha1.Measurement{
		measurementAt(v1alpha1.AnalysisPhaseSuccessful, "1", now.Add(-time.Minute)),
	}
	dup := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "1", now.Add(-time.Minute))
	older := measurementAt(v1alpha1.AnalysisPhaseFailed, "0", now.Add(-2*time.Minute))
	fresh := measurementAt(v1alpha1.AnalysisPhaseFailed, "2", now)

	assert.True(t, shouldDiscardMeasurement(history, dup))
	assert.True(t, shouldDiscardMeasurement(history, older))
	assert.False(t, shouldDiscardMeasurement(history, fresh))
}

func TestConsecutiveFailures(t *testing.T) {
	now := time.Now().UTC()
	at := func(phase v1alpha1.AnalysisPhase, offset time.Duration) v1alpha1.Measurement {
		return measurementAt(phase, "1", now.Add(offset))
	}

	t.Run("all failed", func(t *testing.T) {
		result := v1alpha1.MetricResult{Measurements: []v1alpha1.Measurement{
			at(v1alpha1.AnalysisPhaseFailed, -3*time.Minute),
			at(v1alpha1.AnalysisPhaseFailed, -2*time.Minute),
			at(v1alpha1.AnalysisPhaseFailed, -time.Minute),
		}}
		assert.Equal(t, int32(3), ConsecutiveFailures(result))
	})

	t.Run("success resets the streak", func(t *testing.T) {
		result := v1alpha1.MetricResult{Measurements: []v1alpha1.Measurement{
			at(v1alpha1.AnalysisPhaseFailed, -4*time.Minute),
			at(v1alpha1.AnalysisPhaseFailed, -3*time.Minute),
			at(v1alpha1.AnalysisPhaseSuccessful, -2*time.Minute),
			at(v1alpha1.AnalysisPhaseFailed, -time.Minute),
		}}
		assert.Equal(t, int32(1), ConsecutiveFailures(result))
	})

	t.Run("retryable error breaks the streak without adding to it", func(t *testing.T) {
		result := v1alpha1.MetricResult{Measurements: []v1alpha1.Measurement{
			at(v1alpha1.AnalysisPhaseFailed, -3*time.Minute),
			at(v1alpha1.AnalysisPhaseFailed, -2*time.Minute),
			at(v1alpha1.AnalysisPhaseError, -time.Minute),
		}}
		assert.Equal(t, int32(0), ConsecutiveFailures(result))
	})

	t.Run("inconclusive breaks the streak", func(t *testing.T) {
		result := v1alpha1.MetricResult{Measurements: []v1alpha1.Measurement{
			at(v1alpha1.AnalysisPhaseFailed, -2*time.Minute),
			at(v1alpha1.AnalysisPhaseInconclusive, -time.Minute),
		}}
		assert.Equal(t, int32(0), ConsecutiveFailures(result))
	})

	t.Run("in-flight tail does not mask a failed streak", func(t *testing.T) {
		result := v1alpha1.MetricResult{Measurements: []v1alpha1.Measurement{
			at(v1alpha1.AnalysisPhaseFailed, -time.Minute),
			{Phase: v1alpha1.AnalysisPhaseRunning},
		}}
		assert.Equal(t, int32(1), ConsecutiveFailures(result))
	})

	t.Run("empty history", func(t *testing.T) {
		assert.Equal(t, int32(0), ConsecutiveFailures(v1alpha1.MetricResult{}))
	})
}

func TestIsRetryableMeasurementError(t *testing.T) {
	retryable := []string{
		"Client.Timeout exceeded while awaiting headers",
		"Get \"http://prom:9090/api\": context deadline exceeded",
		"dial tcp 10.0.0.1:9090: connect: connection refused",
		"read tcp: connection reset by peer",
		"HTTP 429 Too Many Requests",
		"lookup prometheus on 1.1.1.1:53: no such host",
		"unexpected EOF",
		"i/o timeout",
	}
	for _, msg := range retryable {
		m := v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseError, Message: msg}
		assert.True(t, IsRetryableMeasurementError(m), "expected retryable: %s", msg)
	}

	nonRetryable := []v1alpha1.Measurement{
		{Phase: v1alpha1.AnalysisPhaseError, Message: "invalid expression: unexpected token"},
		{Phase: v1alpha1.AnalysisPhaseError, Message: "Prometheus metric type not supported"},
		{Phase: v1alpha1.AnalysisPhaseError, Message: ""},
		{Phase: v1alpha1.AnalysisPhaseFailed, Message: "dial tcp: connection refused"},
	}
	for _, m := range nonRetryable {
		assert.False(t, IsRetryableMeasurementError(m), "expected non-retryable: %+v", m)
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestIsRetryableError(t *testing.T) {
	assert.False(t, IsRetryableError(nil))
	assert.True(t, IsRetryableError(context.DeadlineExceeded))
	assert.True(t, IsRetryableError(context.Canceled))
	assert.True(t, IsRetryableError(timeoutErr{}))

	var netErr net.Error = timeoutErr{}
	assert.True(t, IsRetryableError(netErr))
	assert.False(t, IsRetryableError(errors.New("invalid metric result format")))
}

func TestMetricProviderName(t *testing.T) {
	assert.Equal(t, "Prometheus", metricProviderName(v1alpha1.MetricProvider{Prometheus: &v1alpha1.PrometheusMetric{}}))
	assert.Equal(t, "Job", metricProviderName(v1alpha1.MetricProvider{Job: &v1alpha1.JobMetric{}}))
	assert.Equal(t, "Plugin", metricProviderName(v1alpha1.MetricProvider{Plugin: map[string]json.RawMessage{"a": []byte("1")}}))
	assert.Equal(t, "Unknown", metricProviderName(v1alpha1.MetricProvider{}))
}

// TestRunMeasurementsDeduplicatesByProviderID verifies measurements that carry no timestamps but
// share a provider-assigned measurement ID are still deduplicated.
func TestRunMeasurementsDeduplicatesByProviderID(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	c, _, _ := f.newController(noResyncPeriodFunc)

	withID := func() v1alpha1.Measurement {
		return v1alpha1.Measurement{
			Phase:    v1alpha1.AnalysisPhaseSuccessful,
			Value:    "7",
			Metadata: map[string]string{measurementIDMetadataKey: "query-run-42"},
		}
	}

	run := &v1alpha1.AnalysisRun{
		Spec: v1alpha1.AnalysisRunSpec{
			Metrics: []v1alpha1.Metric{{
				Name:     "dedup-id",
				Interval: "60s",
				Provider: v1alpha1.MetricProvider{Job: &v1alpha1.JobMetric{}},
			}},
		},
	}
	f.provider.On("GetMetadata", mock.Anything).Return(map[string]string{})

	f.provider.On("Run", mock.Anything, mock.Anything).Return(withID()).Once()
	firstRun := c.reconcileAnalysisRun(run.DeepCopy())
	assert.Len(t, firstRun.Status.MetricResults[0].Measurements, 1)

	run.Status = *firstRun.Status.DeepCopy()
	f.advance(61 * time.Second)
	f.provider.On("Run", mock.Anything, mock.Anything).Return(withID()).Once()
	secondRun := c.reconcileAnalysisRun(run.DeepCopy())
	assert.Len(t, secondRun.Status.MetricResults[0].Measurements, 1)
	assert.Equal(t, int32(1), secondRun.Status.MetricResults[0].Successful)
	f.provider.AssertExpectations(t)
}

// TestRunMeasurementsDeduplicatesProviderResults verifies a provider returning the same
// observation twice (retry/delayed delivery) only records it once and counters are not inflated.
func TestRunMeasurementsDeduplicatesProviderResults(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	c, _, _ := f.newController(noResyncPeriodFunc)

	base := timeutil.MetaNow().Add(-time.Minute)
	first := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "1", base)
	// the provider hands back the identical observation on the next reconciliation
	duplicate := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "1", base)

	run := &v1alpha1.AnalysisRun{
		Spec: v1alpha1.AnalysisRunSpec{
			Metrics: []v1alpha1.Metric{{
				Name:     "dedup",
				Interval: "60s",
				Provider: v1alpha1.MetricProvider{Job: &v1alpha1.JobMetric{}},
			}},
		},
	}
	f.provider.On("Run", mock.Anything, mock.Anything).Return(first).Once()
	f.provider.On("GetMetadata", mock.Anything).Return(map[string]string{})

	firstRun := c.reconcileAnalysisRun(run.DeepCopy())
	assert.Len(t, firstRun.Status.MetricResults[0].Measurements, 1)
	assert.Equal(t, int32(1), firstRun.Status.MetricResults[0].Successful)

	// second reconciliation returns the duplicate; counters must stay unchanged
	run.Status = *firstRun.Status.DeepCopy()
	f.advance(61 * time.Second)
	f.provider.On("Run", mock.Anything, mock.Anything).Return(duplicate).Once()
	secondRun := c.reconcileAnalysisRun(run.DeepCopy())
	assert.Len(t, secondRun.Status.MetricResults[0].Measurements, 1)
	assert.Equal(t, int32(1), secondRun.Status.MetricResults[0].Successful)
	assert.Equal(t, int32(1), secondRun.Status.MetricResults[0].Count)
	f.provider.AssertExpectations(t)
}

// TestRunMeasurementsIgnoresOutOfOrderResults verifies a delayed, older measurement never
// overwrites the newest accepted result.
func TestRunMeasurementsIgnoresOutOfOrderResults(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	c, _, _ := f.newController(noResyncPeriodFunc)

	base := timeutil.MetaNow().Add(-time.Minute)
	older := measurementAt(v1alpha1.AnalysisPhaseFailed, "1", base.Add(-time.Minute))
	newer := measurementAt(v1alpha1.AnalysisPhaseSuccessful, "2", base)

	run := &v1alpha1.AnalysisRun{
		Spec: v1alpha1.AnalysisRunSpec{
			Metrics: []v1alpha1.Metric{{
				Name:     "ordering",
				Interval: "60s",
				Provider: v1alpha1.MetricProvider{Job: &v1alpha1.JobMetric{}},
			}},
		},
	}
	f.provider.On("Run", mock.Anything, mock.Anything).Return(newer).Once()
	f.provider.On("GetMetadata", mock.Anything).Return(map[string]string{})
	firstRun := c.reconcileAnalysisRun(run.DeepCopy())
	assert.Equal(t, v1alpha1.AnalysisPhaseSuccessful, firstRun.Status.MetricResults[0].Measurements[0].Phase)

	run.Status = *firstRun.Status.DeepCopy()
	f.advance(61 * time.Second)
	f.provider.On("Run", mock.Anything, mock.Anything).Return(older).Once()
	secondRun := c.reconcileAnalysisRun(run.DeepCopy())
	assert.Len(t, secondRun.Status.MetricResults[0].Measurements, 1)
	assert.Equal(t, v1alpha1.AnalysisPhaseSuccessful, secondRun.Status.MetricResults[0].Measurements[0].Phase)
	assert.Equal(t, int32(0), secondRun.Status.MetricResults[0].Failed)
	f.provider.AssertExpectations(t)
}

// TestRunMeasurementsRetryableErrorDoesNotFail verifies a single transient error keeps the run
// going, and a following success clears the consecutive error streak.
func TestRunMeasurementsRetryableErrorDoesNotFail(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	c, _, _ := f.newController(noResyncPeriodFunc)

	transient := v1alpha1.Measurement{
		Phase:      v1alpha1.AnalysisPhaseError,
		Message:    "Get http://prom:9090: context deadline exceeded",
		FinishedAt: nowPtr(),
	}
	success := newMeasurement(v1alpha1.AnalysisPhaseSuccessful)

	run := &v1alpha1.AnalysisRun{
		Spec: v1alpha1.AnalysisRunSpec{
			Metrics: []v1alpha1.Metric{{
				Name:     "flaky",
				Interval: "60s",
				Provider: v1alpha1.MetricProvider{Job: &v1alpha1.JobMetric{}},
			}},
		},
	}
	f.provider.On("GetMetadata", mock.Anything).Return(map[string]string{})

	// one transient error: run stays Running, error counters recorded but no failure
	f.provider.On("Run", mock.Anything, mock.Anything).Return(transient).Once()
	afterError := c.reconcileAnalysisRun(run.DeepCopy())
	result := afterError.Status.MetricResults[0]
	assert.Equal(t, v1alpha1.AnalysisPhaseRunning, afterError.Status.Phase)
	assert.Equal(t, int32(1), result.Error)
	assert.Equal(t, int32(1), result.ConsecutiveError)
	assert.Equal(t, int32(0), result.Failed)

	// a success clears the error streak and the run keeps going
	run.Status = *afterError.Status.DeepCopy()
	f.advance(61 * time.Second)
	f.provider.On("Run", mock.Anything, mock.Anything).Return(success).Once()
	afterSuccess := c.reconcileAnalysisRun(run.DeepCopy())
	result = afterSuccess.Status.MetricResults[0]
	assert.Equal(t, int32(0), result.ConsecutiveError)
	assert.Equal(t, int32(1), result.Error)
	assert.Equal(t, int32(1), result.Successful)
	f.provider.AssertExpectations(t)
}

// TestReconcileAbortsOnlyAfterConsecutiveErrorLimit verifies transient errors below the threshold
// never abort, but sustained errors past it do.
func TestReconcileAbortsOnlyAfterConsecutiveErrorLimit(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	c, _, _ := f.newController(noResyncPeriodFunc)

	limit := int32(2)
	run := &v1alpha1.AnalysisRun{
		Spec: v1alpha1.AnalysisRunSpec{
			Metrics: []v1alpha1.Metric{{
				Name:                  "flaky",
				Interval:              "60s",
				ConsecutiveErrorLimit: int32Ptr(limit),
				Provider:              v1alpha1.MetricProvider{Job: &v1alpha1.JobMetric{}},
			}},
		},
	}
	f.provider.On("GetMetadata", mock.Anything).Return(map[string]string{})
	transient := func() v1alpha1.Measurement {
		return v1alpha1.Measurement{
			Phase:      v1alpha1.AnalysisPhaseError,
			Message:    "dial tcp: connection refused",
			FinishedAt: nowPtr(),
		}
	}

	// first two errors: ConsecutiveError 1 then 2, both <= limit (2), still Running
	for i := 0; i < 2; i++ {
		f.provider.On("Run", mock.Anything, mock.Anything).Return(transient()).Once()
		run = c.reconcileAnalysisRun(run.DeepCopy())
		assert.Equal(t, v1alpha1.AnalysisPhaseRunning, run.Status.Phase, "reconcile %d should not abort", i)
		f.advance(11 * time.Second) // DefaultErrorRetryInterval is 10s
	}
	// third error pushes ConsecutiveError (3) > limit (2) => Error phase
	f.provider.On("Run", mock.Anything, mock.Anything).Return(transient()).Once()
	run = c.reconcileAnalysisRun(run.DeepCopy())
	assert.Equal(t, v1alpha1.AnalysisPhaseError, run.Status.Phase)
	assert.Equal(t, int32(3), run.Status.MetricResults[0].ConsecutiveError)
	f.provider.AssertExpectations(t)
}

// TestReconcileSuccessResetsConsecutiveFailures verifies a success between failures resets the
// consecutive failure streak. With failureLimit=3, the run survives three non-consecutive
// failures (each streak is broken by a success) and only fails when the cumulative total finally
// exceeds the limit; the triggering streak is then a single fresh failure.
func TestReconcileSuccessResetsConsecutiveFailures(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	c, _, _ := f.newController(noResyncPeriodFunc)

	run := &v1alpha1.AnalysisRun{
		Spec: v1alpha1.AnalysisRunSpec{
			Metrics: []v1alpha1.Metric{{
				Name:         "streak",
				Interval:     "60s",
				FailureLimit: int32Ptr(3),
				Provider:     v1alpha1.MetricProvider{Job: &v1alpha1.JobMetric{}},
			}},
		},
	}
	f.provider.On("GetMetadata", mock.Anything).Return(map[string]string{})

	failed := func(v string) v1alpha1.Measurement {
		m := newMeasurement(v1alpha1.AnalysisPhaseFailed)
		m.Value = v
		return m
	}
	ok := func() v1alpha1.Measurement { return newMeasurement(v1alpha1.AnalysisPhaseSuccessful) }
	step := func() { f.advance(61 * time.Second) }

	// failure #1, then a success resets the streak (total failures = 1, still Running)
	f.provider.On("Run", mock.Anything, mock.Anything).Return(failed("1")).Once()
	run = c.reconcileAnalysisRun(run.DeepCopy())
	assert.Equal(t, v1alpha1.AnalysisPhaseRunning, run.Status.Phase)
	assert.Equal(t, int32(1), ConsecutiveFailures(run.Status.MetricResults[0]))
	step()
	f.provider.On("Run", mock.Anything, mock.Anything).Return(ok()).Once()
	run = c.reconcileAnalysisRun(run.DeepCopy())
	assert.Equal(t, int32(0), ConsecutiveFailures(run.Status.MetricResults[0]))

	// failures #2 and #3, each followed by a success, keep the run alive because no streak exceeds
	// the limit and total failures (3) is still within failureLimit
	for _, v := range []string{"2", "3"} {
		step()
		f.provider.On("Run", mock.Anything, mock.Anything).Return(failed(v)).Once()
		run = c.reconcileAnalysisRun(run.DeepCopy())
		assert.Equal(t, v1alpha1.AnalysisPhaseRunning, run.Status.Phase)
		step()
		f.provider.On("Run", mock.Anything, mock.Anything).Return(ok()).Once()
		run = c.reconcileAnalysisRun(run.DeepCopy())
		assert.Equal(t, int32(0), ConsecutiveFailures(run.Status.MetricResults[0]))
	}
	assert.Equal(t, int32(3), run.Status.MetricResults[0].Failed)

	// failure #4 makes total failures (4) exceed failureLimit (3): the run finally fails, and the
	// triggering streak is just this single fresh failure.
	step()
	f.provider.On("Run", mock.Anything, mock.Anything).Return(failed("4")).Once()
	run = c.reconcileAnalysisRun(run.DeepCopy())
	assert.Equal(t, v1alpha1.AnalysisPhaseFailed, run.Status.Phase)
	assert.Equal(t, int32(1), ConsecutiveFailures(run.Status.MetricResults[0]))
	f.provider.AssertExpectations(t)
}

// TestMetricAbortEventContainsTriggerAndProvider verifies the warning event on a failing metric
// records the triggering measurement, provider name and failure counts.
func TestMetricAbortEventContainsTriggerAndProvider(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	c, _, _ := f.newController(noResyncPeriodFunc)

	run := &v1alpha1.AnalysisRun{
		ObjectMeta: metav1.ObjectMeta{Name: "abort-run", Namespace: "default"},
		Spec: v1alpha1.AnalysisRunSpec{
			Metrics: []v1alpha1.Metric{{
				Name:     "abort-metric",
				Provider: v1alpha1.MetricProvider{Job: &v1alpha1.JobMetric{}},
			}},
		},
	}
	f.provider.On("Run", mock.Anything, mock.Anything).Return(newMeasurement(v1alpha1.AnalysisPhaseFailed)).Once()
	f.provider.On("GetMetadata", mock.Anything).Return(map[string]string{})

	newRun := c.reconcileAnalysisRun(run)
	assert.Equal(t, v1alpha1.AnalysisPhaseFailed, newRun.Status.Phase)

	events := f.eventRecorderEvents()
	found := false
	for _, event := range events {
		if event.Reason == "MetricFailed" {
			found = true
			assert.Contains(t, event.Message, "abort-metric")
			assert.Contains(t, event.Message, "Provider:")
			assert.Contains(t, event.Message, "Triggering measurement")
			assert.Contains(t, event.Message, "consecutiveFailed=")
			assert.Contains(t, event.Message, "failed=1")
		}
	}
	assert.True(t, found, "expected a MetricFailed abort event, got %+v", events)
}
