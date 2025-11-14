package cfgreader

import "strings"

func baseName(filename string) string {
	lastDotIndex := strings.LastIndex(filename, ".")
	if lastDotIndex != -1 && lastDotIndex != 0 {
		return filename[:lastDotIndex]
	}
	return filename
}
