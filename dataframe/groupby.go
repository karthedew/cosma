package dataframe

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/compute"
)

// AggKind identifies a reduction applied by GroupBy.Agg.
type AggKind int

const (
	aggSum AggKind = iota
	aggCount
	aggMean
	aggMin
	aggMax
)

// Agg names a column, a reduction, and the output column name. Construct one
// with Sum/Count/Mean/Min/Max and optionally rename it with As.
type Agg struct {
	col   string
	kind  AggKind
	alias string
}

// Sum/Count/Mean/Min/Max build an Agg over col. The default output name is
// "<col>_<op>"; override it with As.
func Sum(col string) Agg   { return Agg{col: col, kind: aggSum, alias: col + "_sum"} }
func Count(col string) Agg { return Agg{col: col, kind: aggCount, alias: col + "_count"} }
func Mean(col string) Agg  { return Agg{col: col, kind: aggMean, alias: col + "_mean"} }
func Min(col string) Agg   { return Agg{col: col, kind: aggMin, alias: col + "_min"} }
func Max(col string) Agg   { return Agg{col: col, kind: aggMax, alias: col + "_max"} }

// As sets the output column name for the aggregation.
func (a Agg) As(name string) Agg { a.alias = name; return a }

// GroupBy is a grouping over one or more key columns, finalized by Agg.
type GroupBy struct {
	df   *DataFrame
	keys []string
}

// GroupBy begins a grouped aggregation over the named key columns.
func (df *DataFrame) GroupBy(keys ...string) *GroupBy {
	return &GroupBy{df: df, keys: keys}
}

// Agg reduces each group with the given aggregations and returns a new
// DataFrame whose columns are the key columns (in group-key order) followed by
// the aggregations (in argument order). Groups appear in first-appearance
// order. Each aggregation is computed once per value column via a single
// per-group fold (compute.GroupReduce).
func (gb *GroupBy) Agg(aggs ...Agg) (*DataFrame, error) {
	df := gb.df
	mem := memory.DefaultAllocator

	// Resolve and box the key columns.
	keyBoxed := make([][]any, len(gb.keys))
	for i, k := range gb.keys {
		idx := df.columnIndex(k)
		if idx < 0 {
			return nil, fmt.Errorf("groupby: key column %q not found", k)
		}
		boxed, err := compute.BoxedValues(df.cols[idx].Chunked())
		if err != nil {
			return nil, fmt.Errorf("groupby: key %q: %w", k, err)
		}
		keyBoxed[i] = boxed
	}

	// Assign group ids in first-appearance order.
	numRows := int(df.NumRows())
	groupIDs := make([]int, numRows)
	index := make(map[string]int)
	var groupKeys [][]any // [group][keyColumn] -> first-seen key value
	for r := 0; r < numRows; r++ {
		tuple := make([]any, len(gb.keys))
		var sb strings.Builder
		for i := range gb.keys {
			v := keyBoxed[i][r]
			tuple[i] = v
			fmt.Fprintf(&sb, "%v\x1f", v)
		}
		key := sb.String()
		id, ok := index[key]
		if !ok {
			id = len(groupKeys)
			index[key] = id
			groupKeys = append(groupKeys, tuple)
		}
		groupIDs[r] = id
	}
	numGroups := len(groupKeys)

	// Reduce each referenced value column once, caching the result.
	reductions := make(map[string][]compute.Aggregates)
	reduceCol := func(col string) ([]compute.Aggregates, error) {
		if r, ok := reductions[col]; ok {
			return r, nil
		}
		idx := df.columnIndex(col)
		if idx < 0 {
			return nil, fmt.Errorf("groupby: aggregation column %q not found", col)
		}
		r, err := compute.GroupReduce(groupIDs, numGroups, df.cols[idx].Chunked())
		if err != nil {
			return nil, fmt.Errorf("groupby: column %q: %w", col, err)
		}
		reductions[col] = r
		return r, nil
	}

	series := make([]*Series, 0, len(gb.keys)+len(aggs))

	// Key columns, rebuilt in group order with their original Arrow types.
	for i, k := range gb.keys {
		vals := make([]any, numGroups)
		for g := 0; g < numGroups; g++ {
			vals[g] = groupKeys[g][i]
		}
		arr, err := compute.BuildArray(df.cols[df.columnIndex(k)].DataType(), vals, mem)
		if err != nil {
			return nil, fmt.Errorf("groupby: key %q: %w", k, err)
		}
		series = append(series, seriesFromArray(k, arr))
	}

	// Aggregation columns.
	for _, a := range aggs {
		red, err := reduceCol(a.col)
		if err != nil {
			return nil, err
		}
		vals := make([]any, numGroups)
		var outType arrow.DataType
		switch a.kind {
		case aggSum:
			outType = df.cols[df.columnIndex(a.col)].DataType()
			for g := range red {
				vals[g] = red[g].Sum
			}
		case aggMin:
			outType = df.cols[df.columnIndex(a.col)].DataType()
			for g := range red {
				vals[g] = red[g].Min
			}
		case aggMax:
			outType = df.cols[df.columnIndex(a.col)].DataType()
			for g := range red {
				vals[g] = red[g].Max
			}
		case aggCount:
			outType = arrow.PrimitiveTypes.Int64
			for g := range red {
				vals[g] = red[g].Count
			}
		case aggMean:
			outType = arrow.PrimitiveTypes.Float64
			for g := range red {
				if red[g].Count > 0 {
					vals[g] = red[g].Mean
				}
			}
		default:
			return nil, fmt.Errorf("groupby: unknown aggregation kind %d", a.kind)
		}
		arr, err := compute.BuildArray(outType, vals, mem)
		if err != nil {
			return nil, fmt.Errorf("groupby: aggregation %q: %w", a.alias, err)
		}
		series = append(series, seriesFromArray(a.alias, arr))
	}

	return New(series)
}

// seriesFromArray wraps a single Arrow array as a one-chunk Series, taking
// ownership of arr (the returned Series's chunked column holds the reference).
func seriesFromArray(name string, arr arrow.Array) *Series {
	ch := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	arr.Release()
	return NewSeriesFromChunked(name, ch)
}
