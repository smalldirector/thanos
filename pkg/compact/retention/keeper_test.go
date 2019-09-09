package retention

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	tlabels "github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestSeriesEvaluator_Evaluate(t *testing.T) {
	defaultPolicy := NewPolicy(make(labels.Labels, 0), 30*time.Hour)
	input := []struct {
		lset labels.Labels
		pg   *policyGroup
	}{
		{
			lset: labels.FromStrings("__name__", "metric_name", "key", "value"),
			pg: &policyGroup{
				active: []*policy{
					defaultPolicy,
				},
				inactive: []*policy{
				},
				isDefaultPolicyActive: true,
			},
		},
		{
			lset: labels.FromStrings("__name__", "metric_name", "key", "value"),
			pg: &policyGroup{
				active: []*policy{
				},
				inactive: []*policy{
					defaultPolicy,
				},
				isDefaultPolicyActive: false,
			},
		},
		{
			lset: labels.FromStrings("__name__", "metric_name", "key", "value"),
			pg: &policyGroup{
				active: []*policy{
					NewPolicy(labels.FromStrings("__name__", "metric_name"), 10*time.Hour),
				},
				inactive: []*policy{
					defaultPolicy,
				},
				isDefaultPolicyActive: false,
			},
		},
		{
			lset: labels.FromStrings("__name__", "metric_name", "key", "value"),
			pg: &policyGroup{
				active: []*policy{
					defaultPolicy,
				},
				inactive: []*policy{
					NewPolicy(labels.FromStrings("__name__", "metric_name"), 40*time.Hour),
				},
				isDefaultPolicyActive: true,
			},
		},
		{
			lset: labels.FromStrings("__name__", "metric_name", "key", "value"),
			pg: &policyGroup{
				active: []*policy{
					defaultPolicy,
				},
				inactive: []*policy{
					NewPolicy(labels.FromStrings("__name__", "different_metric_name"), 40*time.Hour),
				},
				isDefaultPolicyActive: true,
			},
		},
		{
			lset: labels.FromStrings("__name__", "metric_name", "key", "value"),
			pg: &policyGroup{
				active: []*policy{
					defaultPolicy,
					NewPolicy(labels.FromStrings("__name__", "metric_name"), 10*time.Hour),
				},
				inactive: []*policy{
					NewPolicy(labels.FromStrings("key", "value"), 40*time.Hour),
				},
				isDefaultPolicyActive: true,
			},
		},
	}
	expected := []bool{
		true,
		false,
		true,
		false,
		true,
		false,
	}

	evaluator := &seriesEvaluator{}

	for i, v := range input {
		matched, err := evaluator.Evaluate(tlabels.FromMap(v.lset.Map()), v.pg)
		testutil.Ok(t, err)
		testutil.Assert(t, matched == expected[i], "evaluation failure")
	}
}
