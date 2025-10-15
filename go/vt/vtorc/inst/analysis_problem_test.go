/*
Copyright 2025 The Vitess Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package inst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortDetectionAnalysisMatchedProblems(t *testing.T) {
	worstPriority := 10
	testCases := []struct {
		name               string
		in                 []*DetectionAnalysisProblem
		postSortByAnalysis []AnalysisCode
	}{
		{
			name: "default",
			in: []*DetectionAnalysisProblem{
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    InvalidReplica,
						Description: "should be last, not a shardWideAction, priority 1",
						Priority:    detectionAnalysisPriorityMedium,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    InvalidReplica,
						Description: "should be last, not a shardWideAction, worst priority via a PriorityFunc",
					},
					PriorityFunc: func([]*DetectionAnalysisProblem) int {
						return worstPriority
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    PrimaryIsReadOnly,
						Description: "should be after DeadPrimary, not a shardWideAction, priority 0",
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    PrimarySemiSyncMustBeSet,
						Description: "should be after ReplicaSemiSyncMustBeSet, has an after dependency",
					},
					AfterAnalyses: []AnalysisCode{ReplicaSemiSyncMustBeSet},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    ReplicaSemiSyncMustBeSet,
						Description: "should be before PrimarySemiSyncMustBeSet, has a before dependency",
					},
					BeforeAnalyses: []AnalysisCode{PrimarySemiSyncMustBeSet},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:           DeadPrimary,
						Description:        "should be 1st, is a shardWideAction, priority 0",
						HasShardWideAction: true,
					},
				},
			},
			postSortByAnalysis: []AnalysisCode{
				DeadPrimary,
				PrimaryIsReadOnly,
				ReplicaSemiSyncMustBeSet,
				PrimarySemiSyncMustBeSet,
				InvalidReplica,
				InvalidReplica,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sorted := testCase.in
			sortDetectionAnalysisMatchedProblems(sorted)

			require.Len(t, sorted, len(testCase.postSortByAnalysis))
			for i, analysis := range testCase.postSortByAnalysis {
				require.Equal(t, analysis, sorted[i].Meta.Analysis)
			}

			// confirm last problem has the worstPriority
			require.Equal(t, worstPriority, sorted[len(sorted)-1].Meta.Priority)
		})
	}
}
