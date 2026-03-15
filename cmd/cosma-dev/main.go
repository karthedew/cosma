package main

import (
	"fmt"

	"github.com/karthedew/cosma/compute"
	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/internal/expr"
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

	// fmt.Println(df)

	keys := []expr.Expr{
		expr.Lit{Value: "col1"}, // placeholder name for now
	}
	aggs := []expr.Expr{
		expr.Lit{Value: 1},
		expr.Lit{Value: 1},
	}
	_, err = compute.GroupBy(df, keys, aggs)
	if err != nil {
		// expected if your stub returns an error after printing
	}
}
