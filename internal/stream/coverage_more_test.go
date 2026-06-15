package stream

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
)

func TestDataFrameRecordReaderLifecycleAndRecordBatch(t *testing.T) {
	ids, err := dataframe.NewSeries("ids", []int32{1, 2, 3, 4})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{ids})
	require.NoError(t, err)

	rr, err := NewDataFrameRecordReader(df)
	require.NoError(t, err)
	r := rr.(*DataFrameRecordReader)

	r.Retain()
	require.True(t, r.Next())
	require.NotNil(t, r.Record())
	require.Same(t, r.Record(), r.RecordBatch())
	first := r.Record()

	r.Release()
	require.Same(t, first, r.Record())
	require.False(t, r.Next())
	require.NoError(t, r.Err())
	r.Release()
	require.Nil(t, r.Record())
	require.False(t, r.Next())
}

func TestNewDataFrameRecordReaderNilDataFrame(t *testing.T) {
	rr, err := NewDataFrameRecordReader(nil)
	require.Error(t, err)
	require.Nil(t, rr)
}

func TestDataFrameRecordReaderTerminalBranches(t *testing.T) {
	r := &DataFrameRecordReader{refCount: 1}
	r.Release()
	require.Nil(t, r.Record())

	ids, err := dataframe.NewSeries("ids", []int32{1})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{ids})
	require.NoError(t, err)
	rr, err := NewDataFrameRecordReader(df)
	require.NoError(t, err)
	r = rr.(*DataFrameRecordReader)
	require.True(t, r.Next())
	r.Release()
	require.Nil(t, r.Record())

	r = &DataFrameRecordReader{err: errors.New("boom")}
	require.False(t, r.Next())

	r = &DataFrameRecordReader{}
	require.False(t, r.Next())
}
