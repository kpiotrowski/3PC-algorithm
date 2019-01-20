package state

import (
	"encoding/json"
	"fmt"
	"strings"

	zmq "github.com/pebbe/zmq4"
	log "github.com/sirupsen/logrus"
)

const (
	Init     = iota
	Aborted  = iota
	Waiting  = iota
	Prepared = iota
	Commited = iota

	MsgCanCommit = "CAN_COMMIT"
	MsgCommitYes = "COMMIT_YES"
	MsgCommitNo  = "COMMIT_NO"
	MsgPreCommit = "PRE_COMMIT"
	MsgACK       = "ACK"
	MsgDoCommit  = "DO_COMMIT"
	MsgAbort     = "ABORT"
)

type Message struct {
	Sender string `json:"sender"`
	Action string `json:"action"`
}

func (m *Message) IsCanCommit() bool {
	if m.Action == MsgCanCommit {
		return true
	}
	return false
}

func (m *Message) IsYes() bool {
	if m.Action == MsgCommitYes {
		return true
	}
	return false
}

func (m *Message) IsNo() bool {
	if m.Action == MsgCommitNo {
		return true
	}
	return false
}

func (m *Message) IsPreCommit() bool {
	if m.Action == MsgPreCommit {
		return true
	}
	return false
}

func (m *Message) IsACK() bool {
	if m.Action == MsgACK {
		return true
	}
	return false
}

func (m *Message) IsDoCommit() bool {
	if m.Action == MsgDoCommit {
		return true
	}
	return false
}

func (m *Message) IsAbort() bool {
	if m.Action == MsgAbort {
		return true
	}
	return false
}

func Listen(deliverChannel chan Message, address string) {
	receiver, err := zmq.NewSocket(zmq.PULL)
	defer receiver.Close()
	if err != nil {
		log.Error("Failed to create socket: ", err)
	}

	receiver.Bind(fmt.Sprintf("tcp://*:%s", strings.Split(address, ":")[1]))
	for {
		data, err := receiver.RecvBytes(0)
		if err != nil {
			log.Error("Failed receive data: ", err)
			continue
		}

		message := Message{}
		err = json.Unmarshal(data, &message)
		if err != nil {
			log.Error("Failed to decode message", err)
		} else {
			log.Info(fmt.Sprintf("%s received %s from %s", address, message.Action, message.Sender))
			deliverChannel <- message
		}
	}
}

func SendCanCommit(socket *zmq.Socket, sender, receiver string) error {
	log.Info(fmt.Sprintf("%s sends CAN_COMMIT message to %s", sender, receiver))
	payload, _ := json.Marshal(Message{Sender: sender, Action: MsgCanCommit})
	_, err := socket.SendBytes(payload, zmq.DONTWAIT)
	return err
}

func SendCommitYes(socket *zmq.Socket, sender, receiver string) error {
	log.Info(fmt.Sprintf("%s sends COMMIT_YES message to %s", sender, receiver))
	payload, _ := json.Marshal(Message{Sender: sender, Action: MsgCommitYes})
	_, err := socket.SendBytes(payload, zmq.DONTWAIT)
	return err
}

func SendCommitNo(socket *zmq.Socket, sender, receiver string) error {
	log.Info(fmt.Sprintf("%s sends COMMIT_NO message to %s", sender, receiver))
	payload, _ := json.Marshal(Message{Sender: sender, Action: MsgCommitNo})
	_, err := socket.SendBytes(payload, zmq.DONTWAIT)
	return err
}

func SendPreCommit(socket *zmq.Socket, sender, receiver string) error {
	log.Info(fmt.Sprintf("%s sends PRE_COMMIT message to %s", sender, receiver))
	payload, _ := json.Marshal(Message{Sender: sender, Action: MsgPreCommit})
	_, err := socket.SendBytes(payload, zmq.DONTWAIT)
	return err
}

func SendAck(socket *zmq.Socket, sender, receiver string) error {
	log.Info(fmt.Sprintf("%s sends ACK message to %s", sender, receiver))
	payload, _ := json.Marshal(Message{Sender: sender, Action: MsgACK})
	_, err := socket.SendBytes(payload, zmq.DONTWAIT)
	return err
}

func SendDoCommit(socket *zmq.Socket, sender, receiver string) error {
	log.Info(fmt.Sprintf("%s sends DO_COMMIT message to %s", sender, receiver))
	payload, _ := json.Marshal(Message{Sender: sender, Action: MsgDoCommit})
	_, err := socket.SendBytes(payload, zmq.DONTWAIT)
	return err
}

func SendAbort(socket *zmq.Socket, sender, receiver string) error {
	log.Info(fmt.Sprintf("%s sends ABORT message to %s", sender, receiver))
	payload, _ := json.Marshal(Message{Sender: sender, Action: MsgAbort})
	_, err := socket.SendBytes(payload, zmq.DONTWAIT)
	return err
}
