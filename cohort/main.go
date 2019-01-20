package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/kpiotrowski/3PC-algorithm/config"
	"github.com/kpiotrowski/3PC-algorithm/state"
	zmq "github.com/pebbe/zmq4"
	log "github.com/sirupsen/logrus"
)

const logLevel = log.InfoLevel

var (
	canCommit = flag.Bool("c", true, "Can commit response. Default is True. Set to false to simulate commit abort.")
)

func main() {
	flag.Parse()
	log.SetLevel(logLevel)

	if len(flag.Args()) < 2 {
		panic("Not enough arguments to run. You should execute cohort with (flags) [cohort_addr:port] [coordinator_addr:port]")
	}
	c, err := newCohort(flag.Args()[0], flag.Args()[1])
	if err != nil {
		panic(err)
	}

	c.Run()
}

type cohort struct {
	cohortAddr        string
	state             int
	coordinatorSocket *zmq.Socket
	coordinatorAddr   string
	msgChannel        chan state.Message
}

func newCohort(cohortAddr, coordinatorAddr string) (*cohort, error) {
	c := cohort{
		coordinatorAddr: coordinatorAddr,
		cohortAddr:      cohortAddr,
		state:           state.Init,
		msgChannel:      make(chan state.Message),
	}
	var err error
	c.coordinatorSocket, err = zmq.NewSocket(zmq.PUSH)
	if err != nil {
		log.Error("Failed to create socket: ", err)
		return nil, err
	}
	c.coordinatorSocket.Connect(fmt.Sprintf("tcp://%s", coordinatorAddr))
	return &c, nil
}

func (c *cohort) Run() {
	go state.Listen(c.msgChannel, c.cohortAddr)
	defer c.Close()

	log.Info("Cohor starts")
	time.Sleep(config.SleepTime)
	for {
		switch c.state {
		//Init state - wait for the CAN COMMIT message and send YES or NO
		case state.Init:
			c.handleInitState()
		//Waiting state - Wait for the PRE COMMIT message and send ACK
		case state.Waiting:
			c.handleWaitingState()
		//Prepared state - Wait for the Do COMMIT message
		case state.Prepared:
			c.handlePreparedState()
		case state.Commited:
			log.Info("Commit has been commited")
			return
		case state.Aborted:
			log.Warn("Commit has been aborted")
			return
		}
	}
}

func (c *cohort) Close() {
	c.coordinatorSocket.Close()
}

func (c *cohort) abort() {
	c.state = state.Aborted
}

func (c *cohort) handleInitState() {
	for {
		resp := <-c.msgChannel
		//If received abort abort
		if resp.IsAbort() {
			c.abort()
			return
		}
		//If received Can commit send YES or NO
		if resp.IsCanCommit() {
			var err error
			if *canCommit {
				err = state.SendCommitYes(c.coordinatorSocket, c.cohortAddr, c.coordinatorAddr)
			} else {
				err = state.SendCommitNo(c.coordinatorSocket, c.cohortAddr, c.coordinatorAddr)
				c.abort()
				return
			}
			//IF there was an eror - abort
			if err != nil {
				c.abort()
				return
			}
			break
		}
	}
	c.state = state.Waiting
}

func (c *cohort) handleWaitingState() {
	timeout := make(chan bool)
	go func() {
		time.Sleep(config.CohortWaitTime)
		timeout <- true
	}()

loop:
	for {
		select {
		case resp := <-c.msgChannel:
			//If received ABORT - abort
			if resp.IsAbort() {
				c.abort()
				return
			} else if resp.IsPreCommit() {
				//If received PRE commit - send ACK
				state.SendAck(c.coordinatorSocket, c.cohortAddr, c.coordinatorAddr)
				break loop
			}
		//If timeout - abort
		case <-timeout:
			log.Error("Cohort wait TIMEOUT")
			c.abort()
			return
		}
	}
	c.state = state.Prepared
}

func (c *cohort) handlePreparedState() {
	timeout := make(chan bool)
	go func() {
		time.Sleep(config.CohortWaitTime)
		timeout <- true
	}()

loop:
	for {
		select {
		case resp := <-c.msgChannel:
			//If received ABORT - abort
			if resp.IsAbort() {
				c.abort()
				return
			} else if resp.IsDoCommit() {
				//If received DO COMMIT - commit trnsaction
				break loop
			}
		//If timeout - commit
		case <-timeout:
			break loop
		}
	}
	c.state = state.Commited
}
