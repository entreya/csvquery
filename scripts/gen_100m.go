package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
)

func main() {
	file, err := os.Create("100m.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	w := bufio.NewWriterSize(file, 64*1024)

	// Headers
	w.WriteString("id,name,score,active\n")

	// 100,000,000 rows
	limit := 100_000_000
	// To make it faster to generate for quick test, let's just write repeated logic
	// But ensure variety for indexing

	for i := 0; i < limit; i++ {
		// id,name,score,active
		// "1,user1,500,true"
		// Avoid fmt.Sprintf for speed
		w.WriteString(strconv.Itoa(i))
		w.WriteString(",user")
		w.WriteString(strconv.Itoa(i % 1000)) // 1000 unique names
		w.WriteString(",")
		w.WriteString(strconv.Itoa(i % 100)) // 100 unique scores
		w.WriteString(",")
		if i%2 == 0 {
			w.WriteString("true\n")
		} else {
			w.WriteString("false\n")
		}

		if i%1_000_000 == 0 {
			fmt.Printf("\rGenerated %dM rows...", i/1_000_000)
		}
	}
	w.Flush()
	fmt.Println("\nDone: 100m.csv")
}
