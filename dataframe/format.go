package dataframe

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/schema"
)

const (
	maxDisplayRows  = 20
	displayHeadRows = 10
	displayTailRows = 10
)

func (df *DataFrame) String() string {
	if df == nil {
		return "<nil>"
	}
	fields := df.schema.Fields()
	rows := int(df.Height())
	cols := len(fields)

	widths := make([]int, cols)
	for i, f := range fields {
		widths[i] = max(widths[i], strWidth(f.Name))
		widths[i] = max(widths[i], strWidth(dtypeLabel(f)))
	}

	rowIndices := displayRowIndices(rows)
	for _, row := range rowIndices {
		for c := range fields {
			widths[c] = max(widths[c], strWidth(valueAt(df.cols[c], row)))
		}
	}
	if rows > maxDisplayRows {
		for c := range fields {
			widths[c] = max(widths[c], strWidth(ellipsisValue()))
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "shape: (%d, %d)\n", rows, cols)
	writeBorder(&b, "┌", "┬", "┐", widths)
	writeRow(&b, fields, func(i int) string { return fields[i].Name }, widths)
	writeRow(&b, fields, func(int) string { return "---" }, widths)
	writeRow(&b, fields, func(i int) string { return dtypeLabel(fields[i]) }, widths)
	writeBorder(&b, "╞", "╪", "╡", widths)

	if rows > maxDisplayRows {
		for _, row := range rowIndices[:displayHeadRows] {
			writeRow(&b, fields, func(i int) string { return valueAt(df.cols[i], row) }, widths)
		}
		writeRow(&b, fields, func(int) string { return ellipsisValue() }, widths)
		for _, row := range rowIndices[len(rowIndices)-displayTailRows:] {
			writeRow(&b, fields, func(i int) string { return valueAt(df.cols[i], row) }, widths)
		}
	} else {
		for _, row := range rowIndices {
			writeRow(&b, fields, func(i int) string { return valueAt(df.cols[i], row) }, widths)
		}
	}

	writeBorder(&b, "└", "┴", "┘", widths)
	return b.String()
}

func displayRowIndices(rows int) []int {
	if rows <= 0 {
		return nil
	}
	indices := make([]int, rows)
	for i := range indices {
		indices[i] = i
	}
	return indices
}

func ellipsisValue() string {
	return "…"
}

func dtypeLabel(field schema.Field) string {
	switch field.Type {
	case schema.Utf8:
		return "str"
	case schema.Int32:
		return "i32"
	case schema.Int64:
		return "i64"
	case schema.Float32:
		return "f32"
	case schema.Float64:
		return "f64"
	case schema.Bool:
		return "bool"
	default:
		if field.Type != "" {
			return string(field.Type)
		}
		if field.ArrowType != nil {
			return field.ArrowType.Name()
		}
		return "unknown"
	}
}

func valueAt(series Series, row int) string {
	if row < 0 {
		return ""
	}
	chunked := series.Chunked()
	if chunked == nil {
		return ""
	}
	idx := row
	for _, chunk := range chunked.Chunks() {
		if idx < chunk.Len() {
			return valueFromArray(chunk, idx)
		}
		idx -= chunk.Len()
	}
	return ""
}

func valueFromArray(arr arrow.Array, idx int) string {
	if arr == nil {
		return ""
	}
	if arr.IsNull(idx) {
		return "null"
	}
	switch col := arr.(type) {
	case *array.String:
		return col.Value(idx)
	case *array.Int32:
		return strconv.FormatInt(int64(col.Value(idx)), 10)
	case *array.Int64:
		return strconv.FormatInt(col.Value(idx), 10)
	case *array.Float64:
		return strconv.FormatFloat(col.Value(idx), 'f', -1, 64)
	case *array.Float32:
		return strconv.FormatFloat(float64(col.Value(idx)), 'f', -1, 32)
	case *array.Boolean:
		if col.Value(idx) {
			return "true"
		}
		return "false"
	case *array.Timestamp:
		typeInfo, ok := col.DataType().(*arrow.TimestampType)
		if !ok {
			return "<unsupported>"
		}
		return formatTimestamp(col.Value(idx), typeInfo)
	case *array.Date32:
		return formatDate32(col.Value(idx))
	case *array.Date64:
		return formatDate64(col.Value(idx))
	default:
		return "<unsupported>"
	}
}

func formatTimestamp(value arrow.Timestamp, dtype *arrow.TimestampType) string {
	if dtype == nil {
		return "<unsupported>"
	}
	var nanos int64
	switch dtype.Unit {
	case arrow.Second:
		nanos = int64(value) * int64(time.Second)
	case arrow.Millisecond:
		nanos = int64(value) * int64(time.Millisecond)
	case arrow.Microsecond:
		nanos = int64(value) * int64(time.Microsecond)
	case arrow.Nanosecond:
		nanos = int64(value)
	default:
		return "<unsupported>"
	}

	stamp := time.Unix(0, nanos)
	if dtype.TimeZone != "" {
		if loc, err := time.LoadLocation(dtype.TimeZone); err == nil {
			stamp = stamp.In(loc)
		}
	}
	return stamp.Format(time.RFC3339)
}

func formatDate32(value arrow.Date32) string {
	stamp := time.Unix(0, 0).UTC().AddDate(0, 0, int(value))
	return stamp.Format("2006-01-02")
}

func formatDate64(value arrow.Date64) string {
	stamp := time.Unix(0, int64(value)*int64(time.Millisecond)).UTC()
	return stamp.Format("2006-01-02")
}

func writeBorder(b *strings.Builder, left, mid, right string, widths []int) {
	parts := make([]string, len(widths))
	for i, w := range widths {
		parts[i] = strings.Repeat("─", w+2)
	}
	fmt.Fprintf(b, "%s%s%s\n", left, strings.Join(parts, mid), right)
}

func writeRow(b *strings.Builder, fields []schema.Field, value func(int) string, widths []int) {
	parts := make([]string, len(fields))
	for i := range fields {
		parts[i] = " " + padRight(value(i), widths[i]) + " "
	}
	fmt.Fprintf(b, "│%s│\n", strings.Join(parts, "│"))
}

func padRight(value string, width int) string {
	pad := width - strWidth(value)
	if pad <= 0 {
		return value
	}
	return value + strings.Repeat(" ", pad)
}

func strWidth(value string) int {
	return utf8.RuneCountInString(value)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
