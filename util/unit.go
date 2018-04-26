package util

const (
	_                  = iota
	KB                 = 1 << (10 * iota)
	MB
	GB
	DefaultVolSize     = 120 * GB
	TaskWorkerInterval = 1
)

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
