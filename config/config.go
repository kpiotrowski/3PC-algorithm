package config

import "time"

const (
	CoordinatorPort = 8899
	CohortPort      = 8899
)

var (
	SleepTime = time.Second * 2

	CoordinatorWaitTime = time.Second * 20
	CohorWaitTime       = time.Second * 40
)
