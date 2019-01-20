build:
	env GOOS=linux go build -ldflags="-s -w" -o bin/coordinator coordinator/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/cohort cohort/main.go

