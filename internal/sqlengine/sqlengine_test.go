package sqlengine

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTableParams(t *testing.T) {
	input := []string{
		"(",
		"id",
		"INT",
		"PRIMARY KEY",
		",",
		"name",
		"STRING",
		",",
		"lastname",
		"STRING",
		"FOREIGN KEY",
		")",
	}

	expected := []Column{
		{Name: "id", Type: "INT", Extra: "PRIMARY KEY"},
		{Name: "name", Type: "STRING"},
		{Name: "lastname", Type: "STRING", Extra: "FOREIGN KEY"},
	}

	result, err := ParseTableParams(input)
	require.NoError(t, err)

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %+v, got %+v", expected, result)
	}
}
