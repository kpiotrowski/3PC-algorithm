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

var (
	canCommit        = flag.Bool("cc", true, "Can commit response. Default is True. Set to false to simulate commit abort.")
	canCommitTimeout = flag.Bool("cct", false, "Can commit timeout. Default false. Set to true if you want to simulate commit abort.")
	ack              = flag.Bool("ack", true, "If true cohort sends ACK. Default is true. Set to false to simulate commit abort.")
)

func main() {
	flag.Parse()
	log.SetLevel(logLevel)

	if len(os.Args) < 3 {
		panic("Not enough arguments to run. You should execute cohort with [cohort_addr:port] [coordinator_addr:port]")
	}
	c, err := newCohort(os.Args[1], os.Args[2])
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

}

func (c *cohort) handlePreparedState() {

}
