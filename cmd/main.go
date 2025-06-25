package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/alisavch/choudb/internal/sqlengine"
)

func main() {
	file, err := os.OpenFile("./example/test.sql", os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	db := sqlengine.NewDatabase()

	scanLines := bufio.NewScanner(file)
	var sqlRequest strings.Builder

	for scanLines.Scan() {
		line := scanLines.Text()
		if strings.HasPrefix(line, "--") || line == "" {
			break
		}
		sqlRequest.WriteString(line)

		command := strings.TrimSpace(sqlRequest.String())
		sqlRequest.Reset()

		err := db.Execute(command)
		if err != nil {
			fmt.Println(fmt.Errorf("failed to execute command: %w", err))
		}
	}
}
