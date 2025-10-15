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

// DetectionAnalysisProblemInfo contains basic metadata describing a problem.
type DetectionAnalysisProblemInfo struct {
	Analysis           AnalysisCode
	Description        string
	HasShardWideAction bool
}

// DetectionAnalysisProblem describes how to match, sort and track a problem.
type DetectionAnalysisProblem struct {
	Info           DetectionAnalysisProblemInfo
	AfterAnalyses  []AnalysisCode
	BeforeAnalyses []AnalysisCode
	Priority       int
	MatchFunc      func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool
	PriorityFunc   func(allProblems []DetectionAnalysisProblem) int
}

// GetPriority returns the priority of a problem as an int.
func (dap *DetectionAnalysisProblem) GetPriority(allProblems []DetectionAnalysisProblem) int {
	if dap.PriorityFunc != nil {
		return dap.PriorityFunc(allProblems)
	}
	return dap.Priority
}

// HasMatch returns true if a DetectionAnalysisProblem matches the provided states.
func (dap *DetectionAnalysisProblem) HasMatch(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
	if a == nil || ca == nil || dap.MatchFunc == nil {
		return false
	}
	return dap.MatchFunc(a, ca, primaryTablet, tablet, isInvalid)
}

// detectionAnalysisProblems contains all possible problems to match during detection analysis.
var detectionAnalysisProblems = map[AnalysisCode]DetectionAnalysisProblem{
	// InvalidPrimary and InvalidReplica
	InvalidPrimary: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    InvalidPrimary,
			Description: "VTOrc hasn't been able to reach the primary even once since restart/shutdown",
		},
		Priority: 0,
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && isInvalid
		},
	},
	InvalidReplica: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    InvalidReplica,
			Description: "VTOrc hasn't been able to reach the replica even once since restart/shutdown",
		},
		Priority: 1,
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return isInvalid
		},
	},

	// PrimaryDiskStalled
	PrimaryDiskStalled: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:           PrimaryDiskStalled,
			Description:        "Primary has a stalled disk",
			HasShardWideAction: true,
		},
		Priority: 0,
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.IsDiskStalled
		},
	},

	// DeadPrimary*
	DeadPrimaryWithoutReplicas: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:           DeadPrimaryWithoutReplicas,
			Description:        "Primary cannot be reached by vtorc and has no replica",
			HasShardWideAction: true,
		},
		Priority: 0,
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas == 0
		},
	},
	DeadPrimary: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:           DeadPrimary,
			Description:        "Primary cannot be reached by vtorc and none of its replicas is replicating",
			HasShardWideAction: true,
		},
		Priority: 0,
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0
		},
	},
	DeadPrimaryAndReplicas: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:           DeadPrimaryAndReplicas,
			Description:        "Primary cannot be reached by vtorc and none of its replicas is replicating",
			HasShardWideAction: true,
		},
		Priority: 0,
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 && a.CountValidReplicatingReplicas == 0
		},
	},

	// MySQL read-only checks
	PrimaryIsReadOnly: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    PrimaryIsReadOnly,
			Description: "",
		},
		Priority: 0,
	},
	ReplicaIsWritable: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    ReplicaIsWritable,
			Description: "",
		},
		Priority: 1,
	},

	// Semi-sync checks
	PrimarySemiSyncMustBeSet: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    PrimarySemiSyncMustBeSet,
			Description: "Primary semi-sync must be set",
		},
		AfterAnalyses: []AnalysisCode{ReplicaSemiSyncMustBeSet},
		Priority:      0,
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && policy.SemiSyncAckers(ca.durability, tablet) != 0 && !a.SemiSyncPrimaryEnabled
		},
	},
	ReplicaSemiSyncMustBeSet: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    ReplicaSemiSyncMustBeSet,
			Description: "Replica semi-sync must be set",
		},
		BeforeAnalyses: []AnalysisCode{PrimarySemiSyncMustBeSet},
		Priority:       0,
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsPrimary && policy.IsReplicaSemiSync(ca.durability, primaryTablet, tablet) && !a.SemiSyncReplicaEnabled
		},
	},
}

func processDetectionAnalysisMatchedProblems(allProblems []DetectionAnalysisProblem) []DetectionAnalysisProblem {
	// use slices.SortStableFunc because it keeps the original order of equal elements.
	slices.SortStableFunc(allProblems, func(a, b DetectionAnalysisProblem) int {
		aAnalysis := a.Info.Analysis
		bAnalysis := b.Info.Analysis

		// handle dependencies
		if slices.Contains(b.BeforeAnalyses, aAnalysis) || slices.Contains(a.AfterAnalyses, bAnalysis) {
			return 1
		}
		if slices.Contains(a.BeforeAnalyses, bAnalysis) || slices.Contains(b.AfterAnalyses, aAnalysis) {
			return -1
		}

		// prioritize HasShardWideAction
		if !a.Info.HasShardWideAction && b.Info.HasShardWideAction {
			return 1
		}
		if a.Info.HasShardWideAction && !b.Info.HasShardWideAction {
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
	return allProblems
}
