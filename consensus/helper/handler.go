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

package helper

import (
	"fmt"

	"github.com/op/go-logging"
	"github.com/spf13/viper"

	"github.com/hyperledger/fabric/consensus/util"
	"github.com/hyperledger/fabric/core/peer"

	pb "github.com/hyperledger/fabric/protos"
)

var logger *logging.Logger // package-level logger

func init() {
	logger = logging.MustGetLogger("consensus/handler")
}

const (
	// DefaultConsensusQueueSize value of 1000
	DefaultConsensusQueueSize int = 1000
)

// ConsensusHandler handles consensus messages.
// It also implements the Stack.
// 实现了peer MessageHandler接口
// 共识机制ConsensusHandler 内含peer的MessageHandler
type ConsensusHandler struct {
	peer.MessageHandler
	consenterChan chan *util.Message
	coordinator   peer.MessageHandlerCoordinator
}

// NewConsensusHandler constructs a new MessageHandler for the plugin.
// Is instance of peer.HandlerFactory
// 新的共识机制处理器, 验证节点启动时会配置其消息处理方法，如果是验证节点则会根据不同的共识机制加载不同engine的处理方法
// initiatedStream bool，流是否初始化，在节点准备chat时，会先用false来初始化流
// 在handleChat时会获取处理器
func NewConsensusHandler(coord peer.MessageHandlerCoordinator,
	stream peer.ChatStream, initiatedStream bool) (peer.MessageHandler, error) {

	// 先构建节点处理器，后续赋给共识机制处理器
	// peer 的gPRC Chat()实现中initiatedStream 为false
	peerHandler, err := peer.NewPeerHandler(coord, stream, initiatedStream)
	if err != nil {
		return nil, fmt.Errorf("Error creating PeerHandler: %s", err)
	}

	// 共识处理器，同时把上述构建的节点处理器作为其成员
	handler := &ConsensusHandler{
		MessageHandler: peerHandler,
		coordinator:    coord,
	}

	// 共识插件每个连接的最大通信消息数，默认为1000，超过1000则拒绝分发
	consensusQueueSize := viper.GetInt("peer.validator.consensus.buffersize")

	if consensusQueueSize <= 0 {
		logger.Errorf("peer.validator.consensus.buffersize is set to %d, but this must be a positive integer, defaulting to %d", consensusQueueSize, DefaultConsensusQueueSize)
		consensusQueueSize = DefaultConsensusQueueSize
	}

	// 共识机制的处理chan, 初始化1000个消息的channel
	handler.consenterChan = make(chan *util.Message, consensusQueueSize)
	// fanin 扇入（端数）
	// EngineImpl 是一个consensus.Consenter, PeerEndpoint and MessageFan 的结构
	// getEngineImpl() 返回一个 EngineImpl 实例
	getEngineImpl().consensusFan.AddFaninChannel(handler.consenterChan)

	return handler, nil
}

// HandleMessage handles the incoming Fabric messages for the Peer
// 实现peer MessageHandler HandleMessage
// 为节点处理输入的消息，fabric 和openchain有什么区别？处理共识消息，如果不是共识消息交给peer.MessageHandler处理
// 处理共识消息，非共识交给节点的MessageHandler处理
// 验证节点的消息处理方法，非验证节点处理方法为peer.NewPeerHandler
func (handler *ConsensusHandler) HandleMessage(msg *pb.Message) error {
	// 节点消息Message_CHAIN_TRANSACTION后续会转化为共识消息，交给共识插件处理
	if msg.Type == pb.Message_CONSENSUS {
		senderPE, _ := handler.To()
		select {
		// 共识消息写入consenterChan
		// 写入fan.ins 转为只读  fan.out, 在consensus.consenter中处理 externalEventReceiver RecvMsg
		case handler.consenterChan <- &util.Message{
			Msg:    msg,
			Sender: senderPE.ID,
		}:
			return nil
		default:
			err := fmt.Errorf("Message channel for %v full, rejecting", senderPE.ID)
			logger.Errorf("Failed to queue consensus message because: %v", err)
			return err
		}
	}

	if logger.IsEnabledFor(logging.DEBUG) {
		logger.Debugf("Did not handle message of type %s, passing on to next MessageHandler", msg.Type)
	}
	// 节点消息处理调用, 消息非共识消息，交给节点处理
	// 共识处理器，初始化时指定了节点的消息处理器
	return handler.MessageHandler.HandleMessage(msg)
}
