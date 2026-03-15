package stream

import (
	"sync/atomic"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/dataframe"
)

type DataFrameRecordReader struct {
	refCount int64
	iter     *dataframe.RecordBatchIter
	schema   *arrow.Schema
	cur      arrow.Record
	err      error
}

func NewDataFrameRecordReader(df *dataframe.DataFrame) (array.RecordReader, error) {
	iter, err := dataframe.NewRecordBatchIter(df)
	if err != nil {
		return nil, err
	}
	return &DataFrameRecordReader{
		refCount: 1,
		iter:     iter,
		schema:   iter.Schema(),
	}, nil
}

func (r *DataFrameRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *DataFrameRecordReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) != 0 {
		return
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
}

func (r *DataFrameRecordReader) Schema() *arrow.Schema { return r.schema }
func (r *DataFrameRecordReader) Record() arrow.Record  { return r.cur }
func (r *DataFrameRecordReader) Err() error            { return r.err }

func (r *DataFrameRecordReader) Next() bool {
	if r.err != nil || r.iter == nil {
		return false
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}

	rec, ok, err := r.iter.Next()
	if err != nil {
		r.err = err
		return false
	}
	if !ok {
		return false
	}
	r.cur = rec
	return true
}

var _ array.RecordReader = (*DataFrameRecordReader)(nil)
