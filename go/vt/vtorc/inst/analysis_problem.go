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
	"slices"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

const (
	detectionAnalysisPriorityHigh = iota
	detectionAnalysisPriorityMedium
	detectionAnalysisPriorityLow
)

// DetectionAnalysisProblemMeta contains basic metadata describing a problem.
type DetectionAnalysisProblemMeta struct {
	Analysis           AnalysisCode
	Description        string
	HasShardWideAction bool
	Priority           int
}

// DetectionAnalysisProblem describes how to match, sort and track a problem.
type DetectionAnalysisProblem struct {
	Meta           *DetectionAnalysisProblemMeta
	AfterAnalyses  []AnalysisCode
	BeforeAnalyses []AnalysisCode
	MatchFunc      func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool
	PriorityFunc   func(allProblems []*DetectionAnalysisProblem) int
}

// GetPriority returns the priority of a problem as an int. If PriorityFunc is defined, this
// is used to get the priority. Otherwise, the Priority within the 'Meta' field is used.
func (dap *DetectionAnalysisProblem) GetPriority(allProblems []*DetectionAnalysisProblem) int {
	if dap.Meta == nil {
		return 0
	}
	if dap.PriorityFunc != nil {
		dap.Meta.Priority = dap.PriorityFunc(allProblems)
	}
	return dap.Meta.Priority
}

// HasMatch returns true if a DetectionAnalysisProblem matches the provided states.
func (dap *DetectionAnalysisProblem) HasMatch(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
	if a == nil || ca == nil || dap.MatchFunc == nil {
		return false
	}
	return dap.MatchFunc(a, ca, primaryTablet, tablet, isInvalid)
}

// detectionAnalysisProblems contains all possible problems to match during detection analysis.
var detectionAnalysisProblems = map[AnalysisCode]*DetectionAnalysisProblem{
	// InvalidPrimary and InvalidReplica
	InvalidPrimary: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    InvalidPrimary,
			Description: "VTOrc hasn't been able to reach the primary even once since restart/shutdown",
			Priority:    detectionAnalysisPriorityHigh,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && isInvalid
		},
	},
	InvalidReplica: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    InvalidReplica,
			Description: "VTOrc hasn't been able to reach the replica even once since restart/shutdown",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return isInvalid
		},
	},

	// PrimaryDiskStalled
	PrimaryDiskStalled: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:           PrimaryDiskStalled,
			Description:        "Primary has a stalled disk",
			HasShardWideAction: true,
			Priority:           detectionAnalysisPriorityHigh,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.IsDiskStalled
		},
	},

	// DeadPrimary*
	DeadPrimaryWithoutReplicas: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:           DeadPrimaryWithoutReplicas,
			Description:        "Primary cannot be reached by vtorc and has no replica",
			HasShardWideAction: true,
			Priority:           detectionAnalysisPriorityHigh,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas == 0
		},
	},
	DeadPrimary: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:           DeadPrimary,
			Description:        "Primary cannot be reached by vtorc and none of its replicas is replicating",
			HasShardWideAction: true,
			Priority:           detectionAnalysisPriorityHigh,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0
		},
	},
	DeadPrimaryAndReplicas: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:           DeadPrimaryAndReplicas,
			Description:        "Primary cannot be reached by vtorc and none of its replicas is replicating",
			HasShardWideAction: true,
			Priority:           detectionAnalysisPriorityHigh,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 && a.CountValidReplicatingReplicas == 0
		},
	},

	// MySQL read-only checks
	PrimaryIsReadOnly: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimaryIsReadOnly,
			Description: "",
			Priority:    detectionAnalysisPriorityHigh,
		},
	},
	ReplicaIsWritable: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ReplicaIsWritable,
			Description: "",
			Priority:    detectionAnalysisPriorityMedium,
		},
	},

	// Semi-sync checks
	PrimarySemiSyncMustBeSet: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimarySemiSyncMustBeSet,
			Description: "Primary semi-sync must be set",
			Priority:    detectionAnalysisPriorityHigh,
		},
		AfterAnalyses: []AnalysisCode{ReplicaSemiSyncMustBeSet},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && policy.SemiSyncAckers(ca.durability, tablet) != 0 && !a.SemiSyncPrimaryEnabled
		},
	},
	ReplicaSemiSyncMustBeSet: {
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ReplicaSemiSyncMustBeSet,
			Description: "Replica semi-sync must be set",
			Priority:    detectionAnalysisPriorityHigh,
		},
		BeforeAnalyses: []AnalysisCode{PrimarySemiSyncMustBeSet},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsPrimary && policy.IsReplicaSemiSync(ca.durability, primaryTablet, tablet) && !a.SemiSyncReplicaEnabled
		},
	},
}

func sortDetectionAnalysisMatchedProblems(allProblems []*DetectionAnalysisProblem) {
	// use slices.SortStableFunc because it keeps the original order of equal elements.
	slices.SortStableFunc(allProblems, func(a, b *DetectionAnalysisProblem) int {
		if a.Meta == nil || b.Meta == nil {
			return 0 // this should not happen
		}

		// handle before/after dependencies
		aAnalysis := a.Meta.Analysis
		bAnalysis := b.Meta.Analysis
		if slices.Contains(b.BeforeAnalyses, aAnalysis) || slices.Contains(a.AfterAnalyses, bAnalysis) {
			return 1
		}
		if slices.Contains(a.BeforeAnalyses, bAnalysis) || slices.Contains(b.AfterAnalyses, aAnalysis) {
			return -1
		}

		// prioritize HasShardWideAction
		if !a.Meta.HasShardWideAction && b.Meta.HasShardWideAction {
			return 1
		}
		if a.Meta.HasShardWideAction && !b.Meta.HasShardWideAction {
			return -1
		}

		// priority (lower is better)
		aPriority := a.GetPriority(allProblems)
		bPriority := b.GetPriority(allProblems)
		if aPriority > bPriority {
			return 1
		}
		if aPriority < bPriority {
			return -1
		}

		// equal
		return 0
	})
}
