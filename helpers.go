package main

import (
	"strconv"
	"strings"
)

func split2(s, sep string) (string, string) {
	splitResult := strings.SplitN(s, sep, 2)
	if len(splitResult) < 2 {
		return splitResult[0], ""
	}
	return splitResult[0], splitResult[1]
}

func roundToNearestRetention(ts, retention int) int {
	return (ts + retention/2) / retention * retention
}

func rawRetentionToSeconds(rawRetention string) (int, error) {
	retention, err := strconv.Atoi(rawRetention)
	if err == nil {
		return retention, nil
	}

	multiplier := 1
	switch {
	case strings.HasSuffix(rawRetention, "m"):
		multiplier = 60
	case strings.HasSuffix(rawRetention, "h"):
		multiplier = 60 * 60
	case strings.HasSuffix(rawRetention, "d"):
		multiplier = 60 * 60 * 24
	case strings.HasSuffix(rawRetention, "w"):
		multiplier = 60 * 60 * 24 * 7
	case strings.HasSuffix(rawRetention, "y"):
		multiplier = 60 * 60 * 24 * 365
	}

	retention, err = strconv.Atoi(rawRetention[0 : len(rawRetention)-1])
	if err != nil {
		return 0, err
	}

	return retention * multiplier, nil
}
