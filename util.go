package statemachine

import (
	"encoding/json"
	"regexp"
	"time"
)

func deserializeFromJSON(data []byte) (map[string]interface{}, error) {
	var mapData map[string]interface{}
	if err := json.Unmarshal(data, &mapData); err != nil {
		return nil, err
	}
	return mapData, nil
}

func parseTimestamp(timestamp string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", timestamp)
}

func escapeRegexChars(input string) string {
	return regexp.QuoteMeta(input)
}

func CopyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{})
	for k, v := range m {
		vm, ok := v.(map[string]interface{})
		if ok {
			cp[k] = CopyMap(vm)
		} else {
			cp[k] = v
		}
	}

	return cp
}
