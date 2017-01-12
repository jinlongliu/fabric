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
	"sync"
	"time"

	pb "github.com/hyperledger/fabric/protos"
)

//---- event hub framework ----

//handlerListi uses map to implement a set of handlers. use mutex to access
//the map. Note that we don't have lock/unlock wrapper methods as the lock
//of handler list has to be done under the eventProcessor lock. See
//registerHandler, deRegisterHandler. register/deRegister methods
//will be called only when a new consumer chat starts/ends respectively
//and the big lock should have no performance impact
//
type handlerList interface {
	add(ie *pb.Interest, h *handler) (bool, error)
	del(ie *pb.Interest, h *handler) (bool, error)
	foreach(ie *pb.Event, action func(h *handler))
}

type genericHandlerList struct {
	sync.RWMutex
	// key 为处理者句柄
	// value 为布尔值
	handlers map[*handler]bool
}

type chaincodeHandlerList struct {
	sync.RWMutex
	// 第一个string为chaincode id
	// 第二个string为事件类型
	// 第三个为处理者句柄
	handlers map[string]map[string]map[*handler]bool
}

// EventType_CHAINCODE 处理者注册
func (hl *chaincodeHandlerList) add(ie *pb.Interest, h *handler) (bool, error) {
	hl.Lock()
	defer hl.Unlock()

	//chaincode registration info must be non-nil
	// cc注册信息非空
	if ie.GetChaincodeRegInfo() == nil {
		return false, fmt.Errorf("chaincode information not provided for registering")
	}
	//chaincode registration info must be for a non-empty chaincode ID (even if the chaincode does not exist)
	// cc id非空
	if ie.GetChaincodeRegInfo().ChaincodeID == "" {
		return false, fmt.Errorf("chaincode ID not provided for registering")
	}
	//is there a event type map for the chaincode
	// 是否存在
	emap, ok := hl.handlers[ie.GetChaincodeRegInfo().ChaincodeID]
	if !ok {
		emap = make(map[string]map[*handler]bool)
		hl.handlers[ie.GetChaincodeRegInfo().ChaincodeID] = emap
	}

	//create handler map if this is the first handler for the type
	// 定义一个处理者清单
	var handlerMap map[*handler]bool
	if handlerMap, _ = emap[ie.GetChaincodeRegInfo().EventName]; handlerMap == nil {
		// handlerMap为空，说明在chaincodeHandlerList.handlers里对应的cc id里面，没有该事件类型的处理者清单
		// 不存在新创建
		handlerMap = make(map[*handler]bool)
		// 赋给对应的cc id
		emap[ie.GetChaincodeRegInfo().EventName] = handlerMap
	} else if _, ok = handlerMap[h]; ok {

		return false, fmt.Errorf("handler exists for event type")
	}

	//the handler is added to the map
	// 以处理者句柄为key，值为true
	handlerMap[h] = true

	return true, nil
}
func (hl *chaincodeHandlerList) del(ie *pb.Interest, h *handler) (bool, error) {
	hl.Lock()
	defer hl.Unlock()

	//chaincode registration info must be non-nil
	if ie.GetChaincodeRegInfo() == nil {
		return false, fmt.Errorf("chaincode information not provided for de-registering")
	}

	//chaincode registration info must be for a non-empty chaincode ID (even if the chaincode does not exist)
	if ie.GetChaincodeRegInfo().ChaincodeID == "" {
		return false, fmt.Errorf("chaincode ID not provided for de-registering")
	}

	//if there's no event type map, nothing to do
	emap, ok := hl.handlers[ie.GetChaincodeRegInfo().ChaincodeID]
	if !ok {
		return false, fmt.Errorf("chaincode ID not registered")
	}

	//if there are no handlers for the event type, nothing to do
	var handlerMap map[*handler]bool
	if handlerMap, _ = emap[ie.GetChaincodeRegInfo().EventName]; handlerMap == nil {
		return false, fmt.Errorf("event name %s not registered for chaincode ID %s", ie.GetChaincodeRegInfo().EventName, ie.GetChaincodeRegInfo().ChaincodeID)
	} else if _, ok = handlerMap[h]; !ok {
		//the handler is not registered for the event type
		return false, fmt.Errorf("handler not registered for event name %s for chaincode ID %s", ie.GetChaincodeRegInfo().EventName, ie.GetChaincodeRegInfo().ChaincodeID)
	}
	//remove the handler from the map
	delete(handlerMap, h)

	//if the last handler has been removed from handler map for a chaincode's event,
	//remove the event map.
	//if the last map of events have been removed for the chaincode UUID
	//remove the chaincode UUID map
	if len(handlerMap) == 0 {
		delete(emap, ie.GetChaincodeRegInfo().EventName)
		if len(emap) == 0 {
			delete(hl.handlers, ie.GetChaincodeRegInfo().ChaincodeID)
		}
	}

	return true, nil
}

// 从总线上读取的消息只有类型为EventType_CHAINCODE，才会用该方法处理
func (hl *chaincodeHandlerList) foreach(e *pb.Event, action func(h *handler)) {
	hl.Lock()
	defer hl.Unlock()

	//if there's no chaincode event in the event... nothing to do (why was this event sent ?)
	// 事件需要制定cc id
	if e.GetChaincodeEvent() == nil || e.GetChaincodeEvent().ChaincodeID == "" {
		return
	}

	//get the event map for the chaincode
	// 根据事件的chaincode id 获取相关的事件、事件处理器者映射
	if emap := hl.handlers[e.GetChaincodeEvent().ChaincodeID]; emap != nil {
		//get the handler map for the event
		// 获取事件的处理者映射
		if handlerMap := emap[e.GetChaincodeEvent().EventName]; handlerMap != nil {
			for h := range handlerMap {
				action(h)
			}
		}
		//send to handlers who want all events from the chaincode, but only if
		//EventName is not already "" (chaincode should NOT send nameless events though)
		if e.GetChaincodeEvent().EventName != "" {
			if handlerMap := emap[""]; handlerMap != nil {
				for h := range handlerMap {
					// 执行事件处理器的send
					action(h)
				}
			}
		}
	}
}

func (hl *genericHandlerList) add(ie *pb.Interest, h *handler) (bool, error) {
	// 存放一个处理者清单，以处理者句柄为key
	hl.Lock()
	if _, ok := hl.handlers[h]; ok {
		hl.Unlock()
		return false, fmt.Errorf("handler exists for event type")
	}
	hl.handlers[h] = true
	hl.Unlock()
	return true, nil
}

func (hl *genericHandlerList) del(ie *pb.Interest, h *handler) (bool, error) {
	hl.Lock()
	if _, ok := hl.handlers[h]; !ok {
		hl.Unlock()
		return false, fmt.Errorf("handler does not exist for event type")
	}
	delete(hl.handlers, h)
	hl.Unlock()
	return true, nil
}

func (hl *genericHandlerList) foreach(e *pb.Event, action func(h *handler)) {
	hl.Lock()
	for h := range hl.handlers {
		action(h)
	}
	hl.Unlock()
}

//eventProcessor has a map of event type to handlers interested in that
//event type. start() kicks of the event processor where it waits for Events
//from producers. We could easily generalize the one event handling loop to one
//per handlerMap if necessary.
//
type eventProcessor struct {
	sync.RWMutex
	eventConsumers map[pb.EventType]handlerList

	//we could generalize this with mutiple channels each with its own size
	// 相当于事件总线
	eventChannel chan *pb.Event

	//milliseconds timeout for producer to send an event.
	//if < 0, if buffer full, unblocks immediately and not send
	//if 0, if buffer full, will block and guarantee the event will be sent out
	//if > 0, if buffer full, blocks till timeout
	timeout int
}

//global eventProcessor singleton created by initializeEvents. Openchain producers
//send events simply over a reentrant static method
// 单例模式，全局只有一个eventProcessor
var gEventProcessor *eventProcessor

func (ep *eventProcessor) start() {
	producerLogger.Info("event processor started")
	for {
		//wait for event
		// 事件处理器从事件服务器的事件总线读取事件
		e := <-ep.eventChannel

		var hl handlerList
		eType := getMessageType(e)
		ep.Lock()
		// 该事件类型在事件消费者记录内没有，则报错继续循环
		if hl, _ = ep.eventConsumers[eType]; hl == nil {
			producerLogger.Errorf("Event of type %s does not exist", eType)
			ep.Unlock()
			continue
		}
		//lock the handler map lock
		ep.Unlock()

		// 根据cc id获取事件和事件处理者映射，同时调用发送，相当于发送给消费者
		// handlerlist 有两个实现 chaincodeHandlerList   genericHandlerList
		// 根据从消息总线接收到的事件类型的不同调用不同的处理
		// genericHandlerList 对每个处理着都调用发送
		hl.foreach(e, func(h *handler) {
			if e.Event != nil {
				h.SendMessage(e)
			}
		})

	}
}

//initialize and start
func initializeEvents(bufferSize uint, tout int) {
	if gEventProcessor != nil {
		panic("should not be called twice")
	}

	// 构建服务端事件处理器，包含一个事件消费者映射成员：键为事件类型，值为处理者(包含处理方法逻辑)
	// 成员包含一个事件channel
	gEventProcessor = &eventProcessor{eventConsumers: make(map[pb.EventType]handlerList), eventChannel: make(chan *pb.Event, bufferSize), timeout: tout}

	// 增加内部事件，主要是讲事件类型和事件处理者的关系放入内部成员
	addInternalEventTypes()

	//start the event processor
	// 开始时间处理器
	// 相当于启动全局事件服务器
	go gEventProcessor.start()
}

//AddEventType supported event
func AddEventType(eventType pb.EventType) error {
	gEventProcessor.Lock()
	producerLogger.Debugf("registering %s", pb.EventType_name[int32(eventType)])
	if _, ok := gEventProcessor.eventConsumers[eventType]; ok {
		gEventProcessor.Unlock()
		return fmt.Errorf("event type exists %s", pb.EventType_name[int32(eventType)])
	}

	// 将内部事件加入到事件消费者成员中，键为事件类型 值为 处理者清单
	// EventType_CHAINCODE 事件则用chaincodeHandlerList处理
	// EventType_BLOCK，EventType_REJECTION 则用genericHandlerList
	// 以上为内不是事件类型
	switch eventType {
	case pb.EventType_BLOCK:
		gEventProcessor.eventConsumers[eventType] = &genericHandlerList{handlers: make(map[*handler]bool)}
	case pb.EventType_CHAINCODE:
		gEventProcessor.eventConsumers[eventType] = &chaincodeHandlerList{handlers: make(map[string]map[string]map[*handler]bool)}
	case pb.EventType_REJECTION:
		gEventProcessor.eventConsumers[eventType] = &genericHandlerList{handlers: make(map[*handler]bool)}
	}
	gEventProcessor.Unlock()

	return nil
}

// 注册事件处理者
func registerHandler(ie *pb.Interest, h *handler) error {
	producerLogger.Debugf("registerHandler %s", ie.EventType)

	gEventProcessor.Lock()
	defer gEventProcessor.Unlock()
	// 事件处理器的事件消费者记录，获取事件处理者的清单处理方法
	// hl 处理者清单方法
	// EventType_CHAINCODE 事件则用chaincodeHandlerList处理
	// EventType_BLOCK，EventType_REJECTION 则用genericHandlerList
	// hl.add调用将处理者加入记录
	if hl, ok := gEventProcessor.eventConsumers[ie.EventType]; !ok {
		return fmt.Errorf("event type %s does not exist", ie.EventType)
	} else if _, err := hl.add(ie, h); err != nil {
		return fmt.Errorf("error registering handler for  %s: %s", ie.EventType, err)
	}

	return nil
}

func deRegisterHandler(ie *pb.Interest, h *handler) error {
	producerLogger.Debugf("deRegisterHandler %s", ie.EventType)

	gEventProcessor.Lock()
	defer gEventProcessor.Unlock()
	if hl, ok := gEventProcessor.eventConsumers[ie.EventType]; !ok {
		return fmt.Errorf("event type %s does not exist", ie.EventType)
	} else if _, err := hl.del(ie, h); err != nil {
		return fmt.Errorf("error deregistering handler for %s: %s", ie.EventType, err)
	}

	return nil
}

//------------- producer API's -------------------------------

//Send sends the event to interested consumers
// 发送事件到事件总线
func Send(e *pb.Event) error {
	// 单例模式全局一个
	if e.Event == nil {
		producerLogger.Error("event not set")
		return fmt.Errorf("event not set")
	}

	if gEventProcessor == nil {
		return nil
	}

	if gEventProcessor.timeout < 0 {
		select {
		case gEventProcessor.eventChannel <- e:
		default:
			return fmt.Errorf("could not send the blocking event")
		}
	} else if gEventProcessor.timeout == 0 {
		gEventProcessor.eventChannel <- e
	} else {
		select {
		// 写入事件总线
		case gEventProcessor.eventChannel <- e:
		case <-time.After(time.Duration(gEventProcessor.timeout) * time.Millisecond):
			return fmt.Errorf("could not send the blocking event")
		}
	}

	return nil
}
