/*
Copyright 2020 The Vitess Authors.

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

package engine

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*SQLCalcFoundRows)(nil)

// SQLCalcFoundRows is a primitive to execute limit and count query as per their individual plan.
type SQLCalcFoundRows struct {
	LimitPrimitive Primitive
	CountPrimitive Primitive
}

// TryExecute implements the Primitive interface
func (s *SQLCalcFoundRows) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	limitQr, err := vcursor.ExecutePrimitive(ctx, s.LimitPrimitive, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	countQr, err := vcursor.ExecutePrimitive(ctx, s.CountPrimitive, bindVars, false)
	if err != nil {
		return nil, err
	}
	if len(countQr.Rows) != 1 || len(countQr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "count query is not a scalar")
	}
	fr, err := countQr.Rows[0][0].ToCastUint64()
	if err != nil {
		return nil, err
	}
	vcursor.Session().SetFoundRows(fr)
	return limitQr, nil
}

// TryStreamExecute implements the Primitive interface
func (s *SQLCalcFoundRows) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := vcursor.StreamExecutePrimitive(ctx, s.LimitPrimitive, bindVars, wantfields, callback)
	if err != nil {
		return err
	}

	var fr *uint64

	err = vcursor.StreamExecutePrimitive(ctx, s.CountPrimitive, bindVars, wantfields, func(countQr *sqltypes.Result) error {
		if len(countQr.Rows) == 0 && countQr.Fields != nil {
			// this is the fields, which we can ignore
			return nil
		}
		if len(countQr.Rows) != 1 || len(countQr.Rows[0]) != 1 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "count query is not a scalar")
		}
		toUint64, err := countQr.Rows[0][0].ToCastUint64()
		if err != nil {
			return err
		}
		fr = &toUint64
		return nil
	})
	if err != nil {
		return err
	}
	if fr == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "count query for SQL_CALC_FOUND_ROWS never returned a value")
	}
	vcursor.Session().SetFoundRows(*fr)
	return nil
}

// GetFields implements the Primitive interface
func (s *SQLCalcFoundRows) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return s.LimitPrimitive.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (s *SQLCalcFoundRows) NeedsTransaction() bool {
	return s.LimitPrimitive.NeedsTransaction()
}

// Inputs implements the Primitive interface
func (s *SQLCalcFoundRows) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{s.LimitPrimitive, s.CountPrimitive}, nil
}

func (s *SQLCalcFoundRows) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "SQL_CALC_FOUND_ROWS",
	}
}
