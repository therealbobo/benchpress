package utils

import (
	"strings"
)

func NormalizeName(src string) string {
	return strings.ReplaceAll(src, " ", "_")
}
