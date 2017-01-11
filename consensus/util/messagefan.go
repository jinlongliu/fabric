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

package util

import (
	"sync"

	"github.com/op/go-logging"

	pb "github.com/hyperledger/fabric/protos"
)

var logger *logging.Logger // package-level logger

func init() {
	logger = logging.MustGetLogger("consensus/util")
}

// Message encapsulates an OpenchainMessage with sender information
// 结构类似 batchMessage ，一个消息，一个发送者
type Message struct {
	Msg    *pb.Message
	Sender *pb.PeerID
}

// MessageFan contains the reference to the peer's MessageHandlerCoordinator
type MessageFan struct {
	// channel 数组
	ins  []<-chan *Message
	out  chan *Message
	lock sync.Mutex
}

// NewMessageFan will return an initialized MessageFan
func NewMessageFan() *MessageFan {
	return &MessageFan{
		ins: []<-chan *Message{},
		out: make(chan *Message),
	}
}

// AddFaninChannel is intended to be invoked by Handler to add a channel to be fan-ed in
// 被处理器调用将channel fanin
func (fan *MessageFan) AddFaninChannel(channel <-chan *Message) {
	// 互斥锁锁定
	fan.lock.Lock()
	defer fan.lock.Unlock()

	for _, c := range fan.ins {
		// 已经存在重复
		if c == channel {
			// 判断是否存在，存在则返回
			logger.Warningf("Received duplicate connection")
			return
		}
	}

	// 将channel加入fan.ins
	fan.ins = append(fan.ins, channel)

	// 此处使用goroutine执行使得AddFaninChannel调用不用等待下方操作结束
	go func() {
		// channel中的消息写入fan.out
		// for + channel 循环读，当channel关闭时结束
		for msg := range channel {
			fan.out <- msg
		}

		// 阻塞程序，上述为循环读，上述循环结束时执行下方操作
		fan.lock.Lock()
		defer fan.lock.Unlock()

		for i, c := range fan.ins {
			// 将channel从fan.ins移除
			if c == channel {
				fan.ins = append(fan.ins[:i], fan.ins[i+1:]...)
			}
		}
	}()
}

// GetOutChannel returns a read only channel which the registered channels fan into
// 返回一个只读的channel
func (fan *MessageFan) GetOutChannel() <-chan *Message {
	return fan.out
}
