package main

import (
	"fmt"
	"log"
	"os"

	"github.com/karthedew/cosma/dataframe"
)

func main() {
	path := "examples/data/large.csv"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	df, err := dataframe.ReadCSV(path)
	if err != nil {
		log.Fatalf("read csv %q: %v", path, err)
	}

	fmt.Println(df.String())
}
