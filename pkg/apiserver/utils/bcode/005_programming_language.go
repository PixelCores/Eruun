package bcode

// ErrProgrammingLanguageInvalid programming language config is invalid.
var ErrProgrammingLanguageInvalid = NewBcode(400, 32000, "programming language is invalid")

// ErrProgrammingLanguageExists programming language already exists.
var ErrProgrammingLanguageExists = NewBcode(409, 32001, "programming language already exists")

// ErrProgrammingLanguageNotFound programming language not found.
var ErrProgrammingLanguageNotFound = NewBcode(404, 32002, "programming language not found")
