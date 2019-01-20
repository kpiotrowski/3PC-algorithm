package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kpiotrowski/3PC-algorithm/config"
	"github.com/kpiotrowski/3PC-algorithm/state"
	zmq "github.com/pebbe/zmq4"
	log "github.com/sirupsen/logrus"
)

const logLevel = log.InfoLevel

func main() {
	flag.Parse()
	log.SetLevel(logLevel)

	if len(os.Args) < 3 {
		panic("Not enough arguments to run. You should execute coordinator with [coordinator_addr:port] [cohort_addr_1:port] [cohort_addr_1:port] [cohort_addr_1:port] ...")
	}
	c, err := newCoordinator(os.Args[1], os.Args[2:]...)
	if err != nil {
		panic(err)
	}

	c.Run()
}

type coordinator struct {
	coordinatorAddr string
	state           int
	CohortSockets   map[string]*zmq.Socket
	msgChannel      chan state.Message
}

func newCoordinator(coordinatorAddr string, hosts ...string) (*coordinator, error) {
	c := coordinator{
		coordinatorAddr: coordinatorAddr,
		state:           state.Init,
		CohortSockets:   map[string]*zmq.Socket{},
		msgChannel:      make(chan state.Message),
	}

	for _, h := range hosts {
		var err error
		c.CohortSockets[h], err = zmq.NewSocket(zmq.PUSH)
		if err != nil {
			log.Error("Failed to create socket: ", err)
			return nil, err
		}
		c.CohortSockets[h].Connect(fmt.Sprintf("tcp://%s", h))
	}

	return &c, nil
}

func (c *coordinator) Run() {
	go state.Listen(c.msgChannel, c.coordinatorAddr)
	defer c.Close()

	log.Info("Coordinator starts")
	time.Sleep(config.SleepTime)
	for {
		switch c.state {
		//Init state - test CAN commit to all the cohorts
		case state.Init:
			c.handleInitState()
		//Waiting state - wait for all the YES messages and send Pre commit
		case state.Waiting:
			c.handleWaitingState()
		//Prepared state - wait for ACK and send do Commit
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
	// TODO
}

func (c *coordinator) Close() {
	for _, soc := range c.CohortSockets {
		soc.Close()
	}
}

func (c *coordinator) handleInitState() {
	//Sends CAN COMMIT to all the cohorts
	for addr, soc := range c.CohortSockets {
		err := state.SendCanCommit(soc, c.coordinatorAddr, addr)
		//If there was FAIL - abort
		if err != nil {
			c.abort()
			return
		}
		time.Sleep(time.Second)
	}
	//Go to the WAITING state
	c.state = state.Waiting
}

func (c *coordinator) handleWaitingState() {
	responses := map[string]bool{}
	timeout := make(chan bool)

	go func() {
		time.Sleep(config.CoordinatorWaitTime)
		timeout <- true
	}()
	for {
		select {
		case resp := <-c.msgChannel:
			//If received abort or NO - abort
			if resp.IsAbort() || resp.IsNo() {
				c.abort()
				return
			}
			if resp.IsYes() {
				responses[resp.Sender] = true
			}
		//If timeout - abort
		case <-timeout:
			log.Error("Coordinator TIMEOUT")
			c.abort()
			return
		}
		//Received YES from all the cohorts
		if len(responses) == len(c.CohortSockets) {
			break
		}
	}
	c.sendPreCommits()
	c.state = state.Prepared
}

func (c *coordinator) handlePreparedState() {
	responses := map[string]bool{}
	timeout := make(chan bool)

	go func() {
		time.Sleep(config.CoordinatorWaitTime)
		timeout <- true
	}()
	for {
		select {
		case resp := <-c.msgChannel:
			//If received ACK add to the list
			if resp.IsACK() {
				responses[resp.Sender] = true
			}
		//If timeout - abort
		case <-timeout:
			log.Error("Coordinator TIMEOUT")
			c.abort()
			return
		}
		//Received ACK from all the cohorts
		if len(responses) == len(c.CohortSockets) {
			break
		}
	}
	c.sendDoCommits()
	c.state = state.Commited
}

func (c *coordinator) abort() {
	log.Warn("Sending ABORT to all the cohorts")
	for addr, soc := range c.CohortSockets {
		state.SendAbort(soc, c.coordinatorAddr, addr)
	}
	c.state = state.Aborted
}

func (c *coordinator) sendPreCommits() {
	//Sends PRE COMMIT to all the cohorts
	for addr, soc := range c.CohortSockets {
		err := state.SendPreCommit(soc, c.coordinatorAddr, addr)
		//If there was FAIL - abort
		if err != nil {
			c.abort()
			return
		}
		time.Sleep(time.Second)
	}
}

func (c *coordinator) sendDoCommits() {
	//Sends DO COMMIT to all the cohorts
	for addr, soc := range c.CohortSockets {
		_ = state.SendDoCommit(soc, c.coordinatorAddr, addr)
		time.Sleep(time.Second)
	}
}
