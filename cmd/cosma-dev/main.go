package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

func main() {
	fmt.Println("===============================")
	fmt.Println("| Cosma Dev - Build DataFrame |")
	fmt.Println("===============================")
	fmt.Println("")

	category, err := dataframe.NewSeries("category", []string{
		"seafood",
		"meat",
		"fruit",
		"vegetables",
		"vegetables",
		"meat",
		"fruit",
		"fruit",
		"seafood",
		"meat",
		"seafood",
		"seafood",
		"fruit",
		"fruit",
		"vegetables",
		"seafood",
		"seafood",
		"vegetables",
		"vegetables",
		"seafood",
		"meat",
		"fruit",
		"meat",
		"vegetables",
		"fruit",
		"vegetables",
		"seafood",
	})
	if err != nil {
		panic(err)
	}

	calories, err := dataframe.NewSeries("calories", []int32{
		142, 99, 127, 23, 30, 88, 55, 27, 127, 123, 124, 204, 52, 58,
		18, 102, 210, 23, 26, 145, 95, 34, 48, 37, 56, 34, 180,
	})
	if err != nil {
		panic(err)
	}

	fats, err := dataframe.NewSeries("fats_g", []float64{
		6, 4, 0, 0, 0, 5, 0, 0, 1.3, 11, 4, 4, 0, 0, 0, 6, 11, 0, 0, 4,
		5, 0, 2, 0.4, 4.2, 0, 6,
	})
	if err != nil {
		panic(err)
	}

	sugars, err := dataframe.NewSeries("sugars_g", []float64{
		3, 0, 23, 1, 2, 1, 8, 4, 1, 2, 2, 4, 10, 14, 4, 0, 0, 5, 0, 0,
		0, 2, 0, 1, 0, 3, 1,
	})
	if err != nil {
		panic(err)
	}

	series := []*dataframe.Series{category, calories, fats, sugars}
	df, err := dataframe.New(series)
	if err != nil {
		panic(err)
	}

	// Group by category and aggregate the nutrition columns.
	out, err := df.GroupBy("category").Agg(
		context.Background(),
		expr.Col("calories").Count().As("items"),
		expr.Col("calories").Sum().As("total_calories"),
		expr.Col("calories").Mean().As("avg_calories"),
		expr.Col("sugars_g").Max().As("max_sugars"),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("grouped: %v (%d rows)\n", out.Columns(), out.NumRows())
	for _, cat := range stringColumn(out, "category") {
		fmt.Println(" -", cat)
	}
}

// stringColumn collects a string column's values for display.
func stringColumn(df *dataframe.DataFrame, name string) []string {
	col, ok := df.Column(name)
	if !ok {
		return nil
	}
	var out []string
	for _, chunk := range col.Chunked().Chunks() {
		arr := chunk.(*array.String)
		for i := 0; i < arr.Len(); i++ {
			out = append(out, arr.Value(i))
		}
	}
	return out
}
