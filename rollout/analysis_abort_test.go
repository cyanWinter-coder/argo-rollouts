package rollout

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

// buildStepAnalysisRolloutFixture assembles a canary rollout with a step-based analysis run and
// the surrounding replica sets, wiring them into the fixture. The returned run is mutated by the
// caller to set the desired AnalysisRun status.
func buildStepAnalysisRolloutFixture(t *testing.T, f *fixture) (*v1alpha1.Rollout, *v1alpha1.AnalysisTemplate, *v1alpha1.AnalysisRun) {
	t.Helper()
	at := analysisTemplate("abort-at")
	steps := []v1alpha1.CanaryStep{{
		Analysis: &v1alpha1.RolloutAnalysis{
			Templates: []v1alpha1.AnalysisTemplateRef{{TemplateName: at.Name}},
		},
	}}

	r1 := newCanaryRollout("abort-rollout", 1, nil, steps, ptr.To[int32](0), intstr.FromInt(0), intstr.FromInt(1))
	r2 := bumpVersion(r1)
	ar := analysisRun(at, v1alpha1.RolloutTypeStepLabel, r2)

	rs1 := newReplicaSetWithStatus(r1, 1, 1)
	rs2 := newReplicaSetWithStatus(r2, 0, 0)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)
	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	r2 = updateCanaryRolloutStatus(r2, rs1PodHash, 1, 0, 1, false)
	r2.Status.Canary.CurrentStepAnalysisRunStatus = &v1alpha1.RolloutAnalysisRunStatus{Name: ar.Name}

	f.rolloutLister = append(f.rolloutLister, r2)
	f.analysisTemplateLister = append(f.analysisTemplateLister, at)
	f.analysisRunLister = append(f.analysisRunLister, ar)
	f.objects = append(f.objects, r2, at, ar)
	return r2, at, ar
}

// TestRolloutNotAbortedWhileAnalysisRunningWithTransientErrors verifies a still-running
// AnalysisRun that has accumulated retryable (timeout/network) errors below the consecutive error
// threshold does not abort the rollout. The rollout keeps progressing with only the routine status
// patch.
func TestRolloutNotAbortedWhileAnalysisRunningWithTransientErrors(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	r2, _, ar := buildStepAnalysisRolloutFixture(t, f)
	ar.Status = v1alpha1.AnalysisRunStatus{
		Phase: v1alpha1.AnalysisPhaseRunning,
		MetricResults: []v1alpha1.MetricResult{{
			Name:             "success-rate",
			Phase:            v1alpha1.AnalysisPhaseRunning,
			ConsecutiveError: 2,
			Error:            2,
			Failed:           0,
			Metadata:         map[string]string{"providerType": "Prometheus"},
			Measurements: []v1alpha1.Measurement{{
				Phase:   v1alpha1.AnalysisPhaseError,
				Message: "context deadline exceeded",
			}},
		}},
	}

	patchIndex := f.expectPatchRolloutAction(r2)
	f.run(getKey(r2, t))

	patch := f.getPatchedRollout(patchIndex)
	assert.NotContains(t, patch, `"abort":true`)
	assert.NotContains(t, patch, `"phase":"Degraded"`)
}

// TestRolloutAbortedWhenAnalysisConsecutiveErrorLimitExceeded verifies the rollout is aborted once
// the AnalysisRun reaches the Error terminal phase after the consecutive error threshold is
// exceeded, and the abort message carries the analysis run message.
func TestRolloutAbortedWhenAnalysisConsecutiveErrorLimitExceeded(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	r2, _, ar := buildStepAnalysisRolloutFixture(t, f)
	ar.Status = v1alpha1.AnalysisRunStatus{
		Phase:   v1alpha1.AnalysisPhaseError,
		Message: "consecutiveErrors (5) > consecutiveErrorLimit (4)",
		MetricResults: []v1alpha1.MetricResult{{
			Name:             "success-rate",
			Phase:            v1alpha1.AnalysisPhaseError,
			ConsecutiveError: 5,
			Error:            5,
			Metadata:         map[string]string{"providerType": "Prometheus"},
		}},
	}

	patchIndex := f.expectPatchRolloutAction(r2)
	f.run(getKey(r2, t))
	patch := f.getPatchedRollout(patchIndex)

	assert.Contains(t, patch, `"abort":true`)
	assert.Contains(t, patch, `"phase":"Degraded"`)
	assert.Contains(t, patch, "Step-based analysis phase error/failed")
	assert.Contains(t, patch, "consecutiveErrors (5) \\u003e consecutiveErrorLimit (4)")
}

// TestRolloutAbortedWhenAnalysisFailed verifies a Failed AnalysisRun aborts the rollout.
func TestRolloutAbortedWhenAnalysisFailed(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	r2, _, ar := buildStepAnalysisRolloutFixture(t, f)
	ar.Status = v1alpha1.AnalysisRunStatus{
		Phase:   v1alpha1.AnalysisPhaseFailed,
		Message: "failed (1) > failureLimit (0)",
		MetricResults: []v1alpha1.MetricResult{{
			Name:   "success-rate",
			Phase:  v1alpha1.AnalysisPhaseFailed,
			Failed: 1,
		}},
	}

	patchIndex := f.expectPatchRolloutAction(r2)
	f.run(getKey(r2, t))
	patch := f.getPatchedRollout(patchIndex)

	assert.Contains(t, patch, `"abort":true`)
	assert.Contains(t, patch, `"phase":"Degraded"`)
	assert.Contains(t, patch, "Step-based analysis phase error/failed")
}
