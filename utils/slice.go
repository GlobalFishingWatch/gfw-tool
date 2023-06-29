package utils

import "strings"

func ConvertSliceToMap(labels []string) map[string]string {
	result := map[string]string{}

	if labels != nil && len(labels) > 0 {
		for _, label := range labels {
			parts := strings.Split(label, "=")
			if len(parts) != 2 {
				continue
			}
			result[parts[0]] = parts[1]
		}
		return result
	}
	return result
}