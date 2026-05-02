package ir

import (
	"strings"
	"unicode"
)

// Tokenize — простая выделение токенов [a-z0-9]+ + lower ASCII.
func Tokenize(text string) []string {
	var out []string
	i := 0
	for i < len(text) {
		for i < len(text) && !isTokRune(text[i]) {
			i++
		}
		j := i
		for j < len(text) && isTokRune(text[j]) {
			j++
		}
		if i < j {
			out = append(out, strings.ToLower(text[i:j]))
		}
		i = j
	}
	return out
}

func isTokRune(b byte) bool {
	r := rune(b)
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}
