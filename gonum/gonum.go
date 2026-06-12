// Package gonum exports Cosma DataFrames and Series to Gonum matrices and vectors.
//
// All exported functions require numeric columns (int8–int64, uint8–uint64,
// float32, float64). Non-numeric columns are skipped unless explicitly named in
// MatrixOptions.Cols, in which case a non-numeric column is an error.
//
// Null handling must be chosen explicitly via NullPolicy. There is no silent
// behavior: the zero value (NullError) returns an error on the first null cell.
package gonum

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	gonummat "gonum.org/v1/gonum/mat"

	"github.com/karthedew/cosma/dataframe"
)

// NullPolicy controls how null cells are handled during conversion.
type NullPolicy int

const (
	NullError NullPolicy = iota // return error on first null cell (default)
	NullDrop                    // drop entire row if any selected column is null
	NullFill                    // replace null cells with FillValue
)

// MatrixOptions configures ToMatrix.
type MatrixOptions struct {
	// Cols is the ordered list of column names to include. If empty, all numeric
	// columns are included in schema order.
	Cols []string
	// NullPolicy controls null handling. Defaults to NullError.
	NullPolicy NullPolicy
	// FillValue is used when NullPolicy == NullFill.
	FillValue float64
}

// VectorOptions configures ToVector.
type VectorOptions struct {
	NullPolicy NullPolicy
	FillValue  float64
}

// ToMatrix converts numeric columns of df to a *mat.Dense with shape
// (rows × len(cols)). Column order matches MatrixOptions.Cols (or schema order
// if Cols is empty). Columns are extracted in schema order for deterministic output.
func ToMatrix(df *dataframe.DataFrame, opts MatrixOptions) (*gonummat.Dense, error) {
	colNames, err := resolveColumns(df, opts.Cols)
	if err != nil {
		return nil, err
	}
	if len(colNames) == 0 {
		return nil, fmt.Errorf("gonum: no numeric columns in DataFrame")
	}

	// Extract each column as flat float64 slice with null mask.
	type colData struct {
		vals  []float64
		nulls []bool // true = null
	}
	cols := make([]colData, len(colNames))
	var nRows int
	for i, name := range colNames {
		s, ok := df.Column(name)
		if !ok {
			return nil, fmt.Errorf("gonum: column %q not found", name)
		}
		vals, nulls, n, err := extractSeries(&s)
		if err != nil {
			return nil, fmt.Errorf("gonum: column %q: %w", name, err)
		}
		if i == 0 {
			nRows = n
		} else if n != nRows {
			return nil, fmt.Errorf("gonum: column %q has %d rows, expected %d", name, n, nRows)
		}
		cols[i] = colData{vals, nulls}
	}

	// Build row-inclusion set and apply null policy.
	include := make([]bool, nRows)
	for i := range include {
		include[i] = true
	}
	for ci, col := range cols {
		for row, isNull := range col.nulls {
			if !isNull {
				continue
			}
			switch opts.NullPolicy {
			case NullError:
				return nil, fmt.Errorf("gonum: null at column %q row %d", colNames[ci], row)
			case NullDrop:
				include[row] = false
			case NullFill:
				col.vals[row] = opts.FillValue
			}
		}
		cols[ci] = col
	}

	// Count included rows.
	outRows := 0
	for _, ok := range include {
		if ok {
			outRows++
		}
	}
	if outRows == 0 {
		return gonummat.NewDense(0, len(colNames), nil), nil
	}

	// Fill matrix data in row-major order.
	nCols := len(colNames)
	data := make([]float64, outRows*nCols)
	r := 0
	for row := 0; row < nRows; row++ {
		if !include[row] {
			continue
		}
		for c, col := range cols {
			data[r*nCols+c] = col.vals[row]
		}
		r++
	}

	return gonummat.NewDense(outRows, nCols, data), nil
}

// ToVector converts a numeric Series to a *mat.VecDense.
func ToVector(s *dataframe.Series, opts VectorOptions) (*gonummat.VecDense, error) {
	if s == nil {
		return nil, fmt.Errorf("gonum: series is nil")
	}
	if !isNumericType(s.DataType()) {
		return nil, fmt.Errorf("gonum: series %q has non-numeric type %s", s.Name(), s.DataType())
	}
	vals, nulls, n, err := extractSeries(s)
	if err != nil {
		return nil, fmt.Errorf("gonum: series %q: %w", s.Name(), err)
	}

	switch opts.NullPolicy {
	case NullError:
		for i, isNull := range nulls {
			if isNull {
				return nil, fmt.Errorf("gonum: null at series %q row %d", s.Name(), i)
			}
		}
	case NullDrop:
		kept := vals[:0]
		for i, isNull := range nulls {
			if !isNull {
				kept = append(kept, vals[i])
			}
		}
		vals = kept
		n = len(vals)
	case NullFill:
		for i, isNull := range nulls {
			if isNull {
				vals[i] = opts.FillValue
			}
		}
	}

	if n == 0 {
		return gonummat.NewVecDense(0, nil), nil
	}
	return gonummat.NewVecDense(n, vals), nil
}

// resolveColumns returns the ordered column names to use for matrix conversion.
// If names is empty, all numeric columns in schema order are returned.
func resolveColumns(df *dataframe.DataFrame, names []string) ([]string, error) {
	if len(names) > 0 {
		for _, name := range names {
			s, ok := df.Column(name)
			if !ok {
				return nil, fmt.Errorf("gonum: column %q not found", name)
			}
			if !isNumericType(s.DataType()) {
				return nil, fmt.Errorf("gonum: column %q has non-numeric type %s", name, s.DataType())
			}
		}
		return names, nil
	}
	var out []string
	for _, name := range df.Columns() {
		s, _ := df.Column(name)
		if isNumericType(s.DataType()) {
			out = append(out, name)
		}
	}
	return out, nil
}

// extractSeries flattens a Series into a float64 slice with a null mask.
// Returns (vals, nulls, rowCount, error). vals[i] is 0.0 where nulls[i] is true.
func extractSeries(s *dataframe.Series) ([]float64, []bool, int, error) {
	chunked := s.Chunked()
	if chunked == nil {
		return nil, nil, 0, nil
	}
	n := s.Len()
	vals := make([]float64, 0, n)
	nulls := make([]bool, 0, n)
	for _, chunk := range chunked.Chunks() {
		v, nl, err := extractChunk(chunk)
		if err != nil {
			return nil, nil, 0, err
		}
		vals = append(vals, v...)
		nulls = append(nulls, nl...)
	}
	return vals, nulls, len(vals), nil
}

// extractChunk converts one Arrow array chunk to float64 values and null mask.
func extractChunk(chunk arrow.Array) ([]float64, []bool, error) {
	n := chunk.Len()
	vals := make([]float64, n)
	nulls := make([]bool, n)
	for i := 0; i < n; i++ {
		if chunk.IsNull(i) {
			nulls[i] = true
			continue
		}
		v, err := float64At(chunk, i)
		if err != nil {
			return nil, nil, err
		}
		vals[i] = v
	}
	return vals, nulls, nil
}

// float64At extracts the float64 value at index i from a numeric Arrow array.
func float64At(arr arrow.Array, i int) (float64, error) {
	switch a := arr.(type) {
	case *array.Int8:
		return float64(a.Value(i)), nil
	case *array.Int16:
		return float64(a.Value(i)), nil
	case *array.Int32:
		return float64(a.Value(i)), nil
	case *array.Int64:
		return float64(a.Value(i)), nil
	case *array.Uint8:
		return float64(a.Value(i)), nil
	case *array.Uint16:
		return float64(a.Value(i)), nil
	case *array.Uint32:
		return float64(a.Value(i)), nil
	case *array.Uint64:
		return float64(a.Value(i)), nil
	case *array.Float32:
		return float64(a.Value(i)), nil
	case *array.Float64:
		return a.Value(i), nil
	default:
		return 0, fmt.Errorf("unsupported Arrow type %T for float64 extraction", arr)
	}
}

// isNumericType reports whether dt is a supported numeric Arrow type.
func isNumericType(dt arrow.DataType) bool {
	if dt == nil {
		return false
	}
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64:
		return true
	}
	return false
}
