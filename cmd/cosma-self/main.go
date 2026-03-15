package main

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/dataframe"
)

func main() {
	fmt.Println("=========================================")
	fmt.Println("| Cosma Self - Manually Setup DataFrame |")
	fmt.Println("=========================================")

	col1, err := dataframe.NewSeries("col1", []time.Time{
		time.Date(2026, 3, 4, 12, 1, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 2, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 3, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 4, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 5, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 6, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 7, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 8, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 9, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 10, 0, 0, time.UTC),
	})
	if err != nil {
		panic(err)
	}
	col2, err := dataframe.NewSeriesTimestamp("col2", []time.Time{
		time.Date(2026, 3, 4, 12, 1, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 2, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 3, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 4, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 5, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 6, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 7, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 8, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 9, 0, 0, time.UTC),
		time.Date(2026, 3, 4, 12, 10, 0, 0, time.UTC),
	}, arrow.Nanosecond, "UTC")
	if err != nil {
		panic(err)
	}
	fmt.Println(col1)
	fmt.Println(col2)

	series := []*dataframe.Series{col1, col2}
	df, err := dataframe.New(series)
	if err != nil {
		panic(err)
	}

	fmt.Println(df)
}
