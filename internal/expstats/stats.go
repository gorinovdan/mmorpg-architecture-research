package expstats

import (
	"math"
	"sort"
)

func Percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	if p <= 0 {
		return cp[0]
	}
	if p >= 100 {
		return cp[len(cp)-1]
	}
	rank := (p / 100) * float64(len(cp)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return cp[lo]
	}
	frac := rank - float64(lo)
	return cp[lo]*(1-frac) + cp[hi]*frac
}

func DuplicateCount(ids []string) int {
	seen := make(map[string]struct{}, len(ids))
	duplicates := 0
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			duplicates++
			continue
		}
		seen[id] = struct{}{}
	}
	return duplicates
}
