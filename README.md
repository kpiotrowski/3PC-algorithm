# 3PC-algorithm
Simple implementation of the 3 phase commit algorithm. It uses ZeroMQ (PUSH/PULL) for sending messages.

## Build
Make sure that ZeroMQ is installed in you system (version >= 4.0.1).
```
dep ensure
make build
```

## Test

### Run Coordinator

```
./bin/coordinator [coordinator_addr:port] [cohort_addr_1:port] [cohort_addr_2:port]...
```
example:
```
./bin/coordinator 127.0.0.1:8880 127.0.0.1:8881 127.0.0.1:8882
```

### Run Cohorts

```
./bin/cohort [cohort_addr:port] [coordinator_addr:port]
```
example:
```
./bin/cohort 127.0.0.1:8881 127.0.0.1:8880
./bin/cohort 127.0.0.1:8882 127.0.0.1:8880
```

Optionally you can add `-c=false` flag to force Cohort to send NO response on the CAN COMMIT request.
```
./bin/cohort -c=false 127.0.0.1:8881 127.0.0.1:8880
```
