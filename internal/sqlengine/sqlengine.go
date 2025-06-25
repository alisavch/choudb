package sqlengine

import (
	"fmt"
	"strings"
)

type Column struct {
	Name  string
	Type  interface{}
	Extra interface{}
}

type Table struct {
	Columns []Column
	Rows    [][]interface{}
}

type Database struct {
	Tables map[string]*Table
}

func NewDatabase() *Database {
	return &Database{Tables: make(map[string]*Table)}
}

func (db *Database) Execute(query string) error {
	query = strings.TrimSuffix(query, ";")
	tokens := strings.Fields(query)

	if len(tokens) == 0 {
		return fmt.Errorf("empty query")
	}

	switch strings.ToUpper(tokens[0]) {
	case "CREATE":
		return db.execCreateTable(query)
	case "INSERT":
		return db.execInsert(query)
	case "SELECT":
		return db.execSelect(query)
	default:
		return fmt.Errorf("unknown query: %s", tokens[0])
	}
}

func (db *Database) execCreateTable(query string) error {
	// CREATE table users ( int ID, name STRING );
	fmt.Println("executing create table")
	// TODO: validate the query and parentheses there
	fields := strings.Fields(query)
	var paramsStart, paramsEnd int

	for i, field := range fields {
		if field == "(" {
			paramsStart = i
		} else if field == ")" {
			paramsEnd = i
		}
	}

	tableName := fields[2]

	if _, ok := db.Tables[tableName]; ok {
		return fmt.Errorf("table %s already exists", tableName)
	}

	tableParams := fields[paramsStart+1 : paramsEnd]

	columns, err := ParseTableParams(tableParams)
	if err != nil {
		return fmt.Errorf("unable to parse table parameters: %v", err)
	}

	db.Tables[tableName] = &Table{
		Columns: columns,
	}

	fmt.Println("table created")

	return nil
}

func (db *Database) execInsert(query string) error {
	// INSERT INTO users ( 1, 'Hanna' ) ;
	fmt.Println("executing insert")
	fields := strings.Fields(query)
	tableName := fields[2]

	table, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	var paramsStart, paramsEnd int

	for i, field := range fields {
		if field == "(" {
			paramsStart = i
		} else if field == ")" {
			paramsEnd = i
		}
	}

	params := fields[paramsStart+1 : paramsEnd]

	columns := table.Columns

	if len(columns) != len(params) {
		return fmt.Errorf("number of columns does not match number of parameters")
	}

	oneRow := make([]interface{}, 0, len(columns))
	oneRow = append(oneRow, params)

	table.Rows = append(table.Rows, oneRow)

	fmt.Println("one line added", table.Rows)

	return nil
}

func (db *Database) execSelect(query string) error {
	// SELECT * FROM users;
	fmt.Println("executing select")
	fields := strings.Fields(query)
	tableName := fields[len(fields)-1]
	table, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	fmt.Printf("result from %s table: \n columns: %s\n", tableName, table.Columns)

	for _, row := range table.Rows {
		fmt.Printf(" %s\n", row)
	}

	return nil
}

func ParseTableParams(params []string) ([]Column, error) {
	var tokens []string
	var columnsNumber int

	for _, param := range params {
		tok := strings.TrimSpace(param)
		isComma := strings.Contains(tok, ",")
		if isComma {
			columnsNumber++
		}
		tok = strings.Trim(tok, "(),")
		if tok != "" {
			tokens = append(tokens, tok)
		}
	}

	columns := make([]Column, 0, columnsNumber+1)

	for i := 0; i < len(tokens); {
		if i+1 >= len(tokens) {
			break
		}

		col := Column{
			Name: tokens[i],
			Type: tokens[i+1],
		}

		i += 2

		if i < len(tokens) && !isTypeKeyword(tokens[i]) && isExtraKeyword(tokens[i]) {
			col.Extra = tokens[i]
			i++
		}
		columns = append(columns, col)
	}

	return columns, nil
}

func isTypeKeyword(s string) bool {
	switch strings.ToUpper(s) {
	case "INT", "FLOAT", "STRING", "BOOL", "JSON":
		return true
	default:
		return false
	}
}

func isExtraKeyword(s string) bool {
	switch strings.ToUpper(s) {
	case "PRIMARY KEY", "UNIQUE KEY", "FOREIGN KEY":
		return true
	default:
		return false
	}
}
