package main

import (
	"fmt"
	"log"

	"github.com/karthedew/cosma/dataframe"
)

func main() {
	ids, err := dataframe.NewSeries("ids", []int64{1, 2, 3})
	if err != nil {
		log.Fatalf("ids series: %v", err)
	}
	names, err := dataframe.NewSeries("names", []string{"alpha", "beta", "gamma"})
	if err != nil {
		log.Fatalf("names series: %v", err)
	}

	df, err := dataframe.New([]*dataframe.Series{ids, names})
	if err != nil {
		log.Fatalf("dataframe: %v", err)
	}

	fmt.Println(df.String())
}
