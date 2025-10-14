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

type DetectionAnalysisProblemInfo struct {
	Analysis           AnalysisCode
	Description        string
	HasShardWideAction bool
	//Keyspace           string
	//Shard              string
}

type DetectionAnalysisProblem struct {
	Info           DetectionAnalysisProblemInfo
	AfterAnalyses  []AnalysisCode
	BeforeAnalyses []AnalysisCode
	MatchFunc      func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool
}

func (dap *DetectionAnalysisProblem) HasMatch(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
	if a == nil || ca == nil || dap.MatchFunc == nil {
		return false
	}
	return dap.MatchFunc(a, ca, primaryTablet, tablet, isInvalid)
}

func sortDetectionAnalysisMatchedProblems(matchedProblems []DetectionAnalysisProblemInfo) {
	// use slices.SortStableFunc because it keeps the original order of equal elements.
	slices.SortStableFunc(matchedProblems, func(a, b DetectionAnalysisProblemInfo) int {
		aProblem, aOk := detectionAnalysisProblems[a.Analysis]
		bProblem, bOk := detectionAnalysisProblems[b.Analysis]
		if !aOk || !bOk {
			return 0
		}

		// handle dependencies
		if slices.Contains(aProblem.BeforeAnalyses, b.Analysis) || slices.Contains(bProblem.AfterAnalyses, a.Analysis) {
			return 1
		}
		if slices.Contains(aProblem.AfterAnalyses, b.Analysis) || slices.Contains(bProblem.BeforeAnalyses, a.Analysis) {
			return -1
		}

		// prioritize HasShardWideAction
		if !aProblem.Info.HasShardWideAction && bProblem.Info.HasShardWideAction {
			return 1
		}
		if aProblem.Info.HasShardWideAction && !bProblem.Info.HasShardWideAction {
			return -1
		}

		return 0
	})
}

var detectionAnalysisProblems = map[AnalysisCode]DetectionAnalysisProblem{
	// InvalidPrimary and InvalidReplica
	InvalidPrimary: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    InvalidPrimary,
			Description: "VTOrc hasn't been able to reach the primary even once since restart/shutdown",
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && isInvalid
		},
	},
	InvalidReplica: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    InvalidReplica,
			Description: "VTOrc hasn't been able to reach the replica even once since restart/shutdown",
		},
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
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 && a.CountValidReplicatingReplicas == 0
		},
	},

	// Semi-sync
	PrimarySemiSyncMustBeSet: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    PrimarySemiSyncMustBeSet,
			Description: "Primary semi-sync must be set",
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return a.IsClusterPrimary && policy.SemiSyncAckers(ca.durability, tablet) != 0 && !a.SemiSyncPrimaryEnabled
		},
		BeforeAnalyses: []AnalysisCode{ReplicaSemiSyncMustBeSet},
	},
	ReplicaSemiSyncMustBeSet: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    ReplicaSemiSyncMustBeSet,
			Description: "Replica semi-sync must be set",
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsPrimary && policy.IsReplicaSemiSync(ca.durability, primaryTablet, tablet) && !a.SemiSyncReplicaEnabled
		},
		AfterAnalyses: []AnalysisCode{PrimarySemiSyncMustBeSet},
	},
}
