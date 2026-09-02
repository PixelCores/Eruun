package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProgrammingLanguage_EntityContract(t *testing.T) {
	language := &ProgrammingLanguage{ID: "lang-1", Code: "golang", Version: "1.24"}

	require.Equal(t, "eruun_programming_languages", language.TableName())
	require.Equal(t, "programming_language", language.ShortTableName())
	require.Equal(t, "lang-1", language.PrimaryKey())

	index := language.Index()
	require.Equal(t, "lang-1", index["id"])
	require.Equal(t, "golang", index["code"])
	require.Equal(t, "1.24", index["version"])

	require.True(t, builtinModelExists(t, language.TableName()), "expected programming language model in auto-migration snapshot")
}
