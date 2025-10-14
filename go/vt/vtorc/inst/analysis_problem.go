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
	Score int
}

type DetectionAnalysisProblem struct {
	Info           DetectionAnalysisProblemInfo
	AfterAnalyses  []AnalysisCode
	BeforeAnalyses []AnalysisCode
	MatchFunc      func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool
	Priority       int
}

func (dap *DetectionAnalysisProblem) HasMatch(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
	if a == nil || ca == nil || dap.MatchFunc == nil {
		return false
	}
	return dap.MatchFunc(a, ca, primaryTablet, tablet, isInvalid)
}

func processDetectionAnalysisMatchedProblems(matchedProblems []DetectionAnalysisProblemInfo) []DetectionAnalysisProblemInfo {
	processed := matchedProblems
	// use slices.SortStableFunc because it keeps the original order of equal elements.
	slices.SortStableFunc(processed, func(a, b DetectionAnalysisProblemInfo) int {
		aProblem, aOk := detectionAnalysisProblems[a.Analysis]
		bProblem, bOk := detectionAnalysisProblems[b.Analysis]
		if aOk && !bOk {
			return -1
		}
		if !aOk && bOk {
			return 1
		}

		// handle before dependencies
		if len(aProblem.BeforeAnalyses) > 0 && slices.Contains(aProblem.BeforeAnalyses, b.Analysis) {
			return 1
		}
		if len(bProblem.BeforeAnalyses) > 0 && slices.Contains(bProblem.BeforeAnalyses, a.Analysis) {
			return -1
		}

		// handle after dependencies
		if len(aProblem.AfterAnalyses) > 0 && slices.Contains(aProblem.AfterAnalyses, b.Analysis) {
			return 1
		}
		if len(bProblem.AfterAnalyses) > 0 && slices.Contains(bProblem.AfterAnalyses, a.Analysis) {
			return -1
		}

		// prioritize HasShardWideAction
		if !aProblem.Info.HasShardWideAction && bProblem.Info.HasShardWideAction {
			return 1
		}
		if aProblem.Info.HasShardWideAction && !bProblem.Info.HasShardWideAction {
			return -1
		}

		// priority (lower is better)
		if aProblem.Priority > bProblem.Priority {
			return 1
		}
		if aProblem.Priority < bProblem.Priority {
			return -1
		}

		return 0
	})

	return processed
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
		Priority: 0,
	},
	InvalidReplica: {
		Info: DetectionAnalysisProblemInfo{
			Analysis:    InvalidReplica,
			Description: "VTOrc hasn't been able to reach the replica even once since restart/shutdown",
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primaryTablet, tablet *topodatapb.Tablet, isInvalid bool) bool {
			return isInvalid
		},
		Priority: 1,
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
		Priority: 0,
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
		Priority: 0,
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
		Priority: 0,
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
		Priority: 0,
	},

	//
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
		Priority:       0,
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
		Priority:      0,
	},
}
