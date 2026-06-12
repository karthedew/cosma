package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/schema"
)

type RecordBatchOptions struct {
	AllowNullable bool
}

func FromRecordBatches(arrSchema *arrow.Schema, records []arrow.Record) (*DataFrame, error) {
	return FromRecordBatchesWithOptions(arrSchema, records, RecordBatchOptions{})
}

func FromRecordBatchesWithOptions(arrSchema *arrow.Schema, records []arrow.Record, options RecordBatchOptions) (*DataFrame, error) {
	if arrSchema == nil {
		if len(records) == 0 {
			return nil, fmt.Errorf("arrow schema is nil")
		}
		arrSchema = records[0].Schema()
	}

	cosmaSchema, err := schema.FromArrow(arrSchema)
	if err != nil {
		return nil, err
	}
	if options.AllowNullable {
		cosmaSchema = schemaWithNullable(cosmaSchema)
	}

	fields := arrSchema.Fields()
	cols := make([]Series, len(fields))

	if len(records) == 0 {
		for i, field := range fields {
			if field.Type == nil {
				return nil, fmt.Errorf("arrow field %q has nil type", field.Name)
			}
			empty := array.MakeArrayOfNull(memory.DefaultAllocator, field.Type, 0)
			chunked := arrow.NewChunked(field.Type, []arrow.Array{empty})
			cols[i] = *NewSeriesFromChunked(field.Name, chunked)
		}
		return NewDataFrame(cosmaSchema, cols)
	}

	for i, field := range fields {
		if field.Type == nil {
			return nil, fmt.Errorf("arrow field %q has nil type", field.Name)
		}
		arrays := make([]arrow.Array, 0, len(records))
		for _, rec := range records {
			if int(rec.NumCols()) != len(fields) {
				return nil, fmt.Errorf("record columns (%d) != schema fields (%d)", rec.NumCols(), len(fields))
			}
			col := rec.Column(i)
			if col == nil {
				return nil, fmt.Errorf("record column %d is nil", i)
			}
			col.Retain()
			arrays = append(arrays, col)
		}

		chunked := arrow.NewChunked(field.Type, arrays)
		cols[i] = *NewSeriesFromChunked(field.Name, chunked)
	}

	return NewDataFrame(cosmaSchema, cols)
}

func schemaWithNullable(s *schema.Schema) *schema.Schema {
	if s == nil {
		return nil
	}
	fields := s.Fields()
	for i := range fields {
		fields[i].Nullable = true
	}
	return schema.New(fields...)
}
