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

package pbft

import (
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric/consensus"
	pb "github.com/hyperledger/fabric/protos"
)

type communicator interface {
	consensus.Communicator
	consensus.Inquirer
}

// 共识插件的广播器，会调用Helper的广播器，最后调用Impl的广播器
type broadcaster struct {
	comm communicator

	f                int
	broadcastTimeout time.Duration
	msgChans         map[uint64]chan *sendRequest
	closed           sync.WaitGroup
	closedCh         chan struct{}
}

// 发送给peer的消息
type sendRequest struct {
	msg  *pb.Message
	done chan bool
}

// 创建新广播员
func newBroadcaster(self uint64, N int, f int, broadcastTimeout time.Duration, c communicator) *broadcaster {
	// c 为 consensus.Helper
	queueSize := 10 // XXX increase after testing
	// N 网络中验证节点最大数
	// f 网络中容忍最大错误数

	// 消息总线
	chans := make(map[uint64]chan *sendRequest)
	b := &broadcaster{
		comm:             c,
		f:                f,
		broadcastTimeout: broadcastTimeout,
		msgChans:         chans,
		closedCh:         make(chan struct{}),
	}
	// 网络中最多N个验证节点，所以创建N个sendReques通道
	for i := 0; i < N; i++ {
		if uint64(i) == self {
			continue
		}
		chans[uint64(i)] = make(chan *sendRequest, queueSize)
	}

	// We do not start the go routines in the above loop to avoid concurrent map read/writes
	// 启动go routines并发
	for i := 0; i < N; i++ {
		if uint64(i) == self {
			continue
		}
		go b.drainer(uint64(i))
	}

	return b
}

func (b *broadcaster) Close() {
	close(b.closedCh)
	b.closed.Wait()
}

func (b *broadcaster) Wait() {
	b.closed.Wait()
}

func (b *broadcaster) drainerSend(dest uint64, send *sendRequest, successLastTime bool) bool {
	// Note, successLastTime is purely used to avoid flooding the log with unnecessary warning messages when a network problem is encountered
	defer func() {
		b.closed.Done()
	}()
	// 获取验证节点句柄
	h, err := getValidatorHandle(dest)
	if err != nil {
		// 节点句柄获取失败
		if successLastTime {
			logger.Warningf("could not get handle for replica %d", dest)
		}
		send.done <- false
		return false
	}

	// comm 为 consensus.Helper 方法 Unicast 调用了Impl的方法
	// Helper.coordinator 为peer Impl
	// Impl.Unicast 调用peer.Handler SendMessage
	err = b.comm.Unicast(send.msg, h)
	if err != nil {
		if successLastTime {
			logger.Warningf("could not send to replica %d: %v", dest, err)
		}
		send.done <- false
		return false
	}

	send.done <- true
	return true

}
// drainer排水器，将消息总线里面的消息取出处理掉
func (b *broadcaster) drainer(dest uint64) {
	successLastTime := false
	destChan, exsit := b.msgChans[dest] // Avoid doing the map lookup every send
	if !exsit {
		logger.Warningf("could not get message channel for replica %d", dest)
		return
	}

	// 永久循环读取本地msgChans，并发送通过Helper Unicast 再调用 Impl的Unicast发送给节点
	for {
		select {
		case send := <-destChan:
			// 读取通道destChan到send，并调用drainerSend
			successLastTime = b.drainerSend(dest, send, successLastTime)
		case <-b.closedCh:
			// 读取到关闭通道
			for {
				// Drain the message channel to free calling waiters before we shut down
				select {
				case send := <-destChan:
					send.done <- false
					b.closed.Done()
				default:
					return
				}
			}
		}
	}
}

// 单播一个消息，写入chan
// 将一个消息写入目标channel
func (b *broadcaster) unicastOne(msg *pb.Message, dest uint64, wait chan bool) {
	// 写入一个消息到本地通道
	select {
	case b.msgChans[dest] <- &sendRequest{
		msg:  msg,
		done: wait,
	}:
	default:
		// If this channel is full, we must discard the message and flag it as done
		// 目标channel已满，写入失败，在wait内写入一个false，同时完成数-1
		wait <- false
		b.closed.Done()
	}
}
// 发送消息给其它节点，广播
func (b *broadcaster) send(msg *pb.Message, dest *uint64) error {
	select {
	case <-b.closedCh:
		return fmt.Errorf("broadcaster closed")
	default:
	}

	var destCount int
	var required int
	// 如果dest等于nil则说明为广播发送
	if dest != nil {
		// 单播
		destCount = 1
		required = 1
	} else {
		// 组播
		destCount = len(b.msgChans)
		// b.f 网络中容忍最大错误数，则required为最低完成数
		required = destCount - b.f
	}

	// 设置了目标个数的布尔channel
	wait := make(chan bool, destCount)

	if dest != nil {
		// 新增等待1
		b.closed.Add(1)
		b.unicastOne(msg, *dest, wait)
	} else {
		// 广播发送
		// 新增目标个数个等待
		b.closed.Add(len(b.msgChans))
		// 所有消息通道循环，利用单播实现广播
		for i := range b.msgChans {
			// 单一传播1个
			b.unicastOne(msg, i, wait)
		}
	}

	succeeded := 0
	timer := time.NewTimer(b.broadcastTimeout)

	// This loop will try to send, until one of:
	// a) the required number of sends succeed
	// b) all sends complete regardless of success
	// c) the timeout expires and the required number of sends have returned
outer:
	for i := 0; i < destCount; i++ {
		select {
		case success := <-wait:
			if success {
				succeeded++
				if succeeded >= required {
					// 目标成功数达到
					break outer
				}
			}
		case <-timer.C:
			// 超时，有要求数量的反馈，不管成功或者失败
			for i := i; i < required; i++ {
				<-wait
			}
			break outer
		}
	}

	return nil
}

// pbft实现consensus.Communicator
func (b *broadcaster) Unicast(msg *pb.Message, dest uint64) error {
	return b.send(msg, &dest)
}

func (b *broadcaster) Broadcast(msg *pb.Message) error {
	// 广播发送，msg为proto.Marshal后的结果, 接收者？
	return b.send(msg, nil)
}
