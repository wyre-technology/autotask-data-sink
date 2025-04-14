package autotask

import (
	"fmt"
)

// parseBoolValue parses a string value into a boolean
func parseBoolValue(valueStr string) (bool, error) {
	switch valueStr {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", valueStr)
	}
}
