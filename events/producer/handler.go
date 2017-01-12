/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package producer

import (
	"fmt"
	"strconv"

	pb "github.com/hyperledger/fabric/protos"
)

type handler struct {
	ChatStream       pb.Events_ChatServer
	interestedEvents map[string]*pb.Interest
}

func newEventHandler(stream pb.Events_ChatServer) (*handler, error) {
	// 事件处理着初始化
	d := &handler{
		ChatStream: stream,
	}
	// 感兴趣事件记录
	d.interestedEvents = make(map[string]*pb.Interest)
	return d, nil
}

// Stop stops this handler
func (d *handler) Stop() error {
	d.deregisterAll()
	d.interestedEvents = nil
	return nil
}

func getInterestKey(interest pb.Interest) string {
	var key string
	switch interest.EventType {
	case pb.EventType_BLOCK:
		key = "/" + strconv.Itoa(int(pb.EventType_BLOCK))
	case pb.EventType_REJECTION:
		key = "/" + strconv.Itoa(int(pb.EventType_REJECTION))
	case pb.EventType_CHAINCODE:
		key = "/" + strconv.Itoa(int(pb.EventType_CHAINCODE)) + "/" + interest.GetChaincodeRegInfo().ChaincodeID + "/" + interest.GetChaincodeRegInfo().EventName
	default:
		producerLogger.Errorf("unknown interest type %s", interest.EventType)
	}
	return key
}

// 服务器将感兴趣事件类型记录在案
func (d *handler) register(iMsg []*pb.Interest) error {
	// Could consider passing interest array to registerHandler
	// and only lock once for entire array here
	for _, v := range iMsg {
		// 在事件处理器中注册事件处理者
		if err := registerHandler(v, d); err != nil {
			producerLogger.Errorf("could not register %s: %s", v, err)
			continue
		}
		// 注册感兴趣事件 EventType_REGISTER EventType_BLOCK EventType_CHAINCODE
		// 以内部事件类型为键，以消费者感兴趣的事件类型为值
		// 同时把客户端感兴趣的事件加入到处理者的记录
		d.interestedEvents[getInterestKey(*v)] = v
	}

	return nil
}

func (d *handler) deregister(iMsg []*pb.Interest) error {
	for _, v := range iMsg {
		if err := deRegisterHandler(v, d); err != nil {
			producerLogger.Errorf("could not deregister %s", v)
			continue
		}
		delete(d.interestedEvents, getInterestKey(*v))
	}
	return nil
}

func (d *handler) deregisterAll() {
	for k, v := range d.interestedEvents {
		if err := deRegisterHandler(v, d); err != nil {
			producerLogger.Errorf("could not deregister %s", v)
			continue
		}
		delete(d.interestedEvents, k)
	}
}

// HandleMessage handles the Openchain messages for the Peer.
func (d *handler) HandleMessage(msg *pb.Event) error {
	//producerLogger.Debug("Handling Event")
	switch msg.Event.(type) {
	// src/github.com/hyperledger/fabric/events/consumer/consumer.go:73
	// 客户端注册时将感兴趣的事件类型封装为Event_Register
	case *pb.Event_Register:
		// 获取所有感兴趣的事件
		eventsObj := msg.GetRegister()
		if err := d.register(eventsObj.Events); err != nil {
			return fmt.Errorf("Could not register events %s", err)
		}
	case *pb.Event_Unregister:
		// 取消关注处理
		eventsObj := msg.GetUnregister()
		if err := d.deregister(eventsObj.Events); err != nil {
			return fmt.Errorf("Could not unregister events %s", err)
		}
	case nil:
	default:
		return fmt.Errorf("Invalide type from client %T", msg.Event)
	}
	//TODO return supported events.. for now just return the received msg
	if err := d.ChatStream.Send(msg); err != nil {
		return fmt.Errorf("Error sending response to %v:  %s", msg, err)
	}

	return nil
}

// SendMessage sends a message to the remote PEER through the stream
// 发送消息给消费者
func (d *handler) SendMessage(msg *pb.Event) error {
	err := d.ChatStream.Send(msg)
	if err != nil {
		return fmt.Errorf("Error Sending message through ChatStream: %s", err)
	}
	return nil
}
