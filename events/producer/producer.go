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
	"io"
	"time"

	pb "github.com/hyperledger/fabric/protos"
	"github.com/op/go-logging"
)

const defaultTimeout = time.Second * 3

var producerLogger = logging.MustGetLogger("eventhub_producer")

// EventsServer implementation of the Peer service
type EventsServer struct {
}

//singleton - if we want to create multiple servers, we need to subsume events.gEventConsumers into EventsServer
var globalEventsServer *EventsServer

// NewEventsServer returns a EventsServer
// 事件服务器
func NewEventsServer(bufferSize uint, timeout int) *EventsServer {
	if globalEventsServer != nil {
		panic("Cannot create multiple event hub servers")
	}
	// 全局事件服务器
	globalEventsServer = new(EventsServer)
	initializeEvents(bufferSize, timeout)
	//initializeCCEventProcessor(bufferSize, timeout)
	return globalEventsServer
}

// Chat implementation of the the Chat bidi streaming RPC function
// gRPC服务端实现，供gRPC客户端调用
// src/github.com/hyperledger/fabric/events/consumer/consumer.go:222
func (p *EventsServer) Chat(stream pb.Events_ChatServer) error {
	// gRPC提供流stream供客户端服务端读写
	// 事件服务器启动时，启动了一个事件处理器
	// 每次客户端发来一次请求，就通过和客户端的stream，生成一个事件处理者
	handler, err := newEventHandler(stream)
	if err != nil {
		return fmt.Errorf("Error creating handler during handleChat initiation: %s", err)
	}
	defer handler.Stop()
	for {
		// 服务端接收到客户端的消息
		// 主要应用场景是服务端发客户端感兴趣的事件给客户端，但是在注册阶段客户端会发送一些注册事件给服务器端
		in, err := stream.Recv()
		if err == io.EOF {
			producerLogger.Debug("Received EOF, ending Chat")
			return nil
		}
		if err != nil {
			e := fmt.Errorf("Error during Chat, stopping handler: %s", err)
			producerLogger.Error(e.Error())
			return e
		}
		// 用处理器处理
		err = handler.HandleMessage(in)
		if err != nil {
			producerLogger.Errorf("Error handling message: %s", err)
			return err
		}

	}
}
