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
	testCases := []struct {
		name               string
		in                 []DetectionAnalysisProblemInfo
		postSortByAnalysis []AnalysisCode
	}{
		{
			name: "default",
			in: []DetectionAnalysisProblemInfo{
				{
					Analysis:    InvalidReplica,
					Description: "should be after DeadPrimary, not a shardWideAction",
				},
				{
					Analysis:    PrimarySemiSyncMustBeSet,
					Description: "should be after ReplicaSemiSyncMustBeSet",
				},
				{
					Analysis:    ReplicaSemiSyncMustBeSet,
					Description: "should be before PrimarySemiSyncMustBeSet",
				},
				{
					Analysis:           DeadPrimary,
					Description:        "should be 1st, is a shardWideAction",
					HasShardWideAction: true,
				},
			},
			postSortByAnalysis: []AnalysisCode{
				DeadPrimary,
				InvalidReplica,
				ReplicaSemiSyncMustBeSet,
				PrimarySemiSyncMustBeSet,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			problems := testCase.in
			sortDetectionAnalysisMatchedProblems(problems)
			for i, analysis := range testCase.postSortByAnalysis {
				require.Equal(t, string(analysis), string(problems[i].Analysis))
			}
		})
	}
}
