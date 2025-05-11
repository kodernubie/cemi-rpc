package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/alitto/pond/v2"
	cemirpc "github.com/kodernubie/cemi-rpc"
	"github.com/oklog/ulid/v2"
	"github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CloseListener func(err error)

type Options struct {
	URL string
}

type RabbitConnection struct {
	options    Options
	conn       *amqp.Connection
	defaultChn *amqp.Channel

	rpcOnce       sync.Once
	rpcQueueName  string
	rpcMutex      sync.RWMutex
	waitChan      map[string]chan []byte
	closeListener []CloseListener
}

var exchangeCreated map[string]bool = map[string]bool{}
var queueCreated map[string]bool = map[string]bool{}

var mutex sync.RWMutex

func New(options Options) cemirpc.Connection {

	conn, defaultChn, err := connect(options)

	if err != nil {
		return nil
	}

	ret := &RabbitConnection{
		options:       options,
		conn:          conn,
		defaultChn:    defaultChn,
		rpcQueueName:  "RPC.REPLY." + ulid.Make().String(),
		waitChan:      map[string]chan []byte{},
		closeListener: []CloseListener{},
	}

	ret.setupCloseNotify()

	return ret
}

func connect(options Options) (*amqp.Connection, *amqp.Channel, error) {

	var err error
	conn, err := amqp.Dial(options.URL)

	if err != nil {
		log.Println("Error dial rabbitmq :", err)
		return nil, nil, err
	}

	defaultChn, err := conn.Channel()

	if err != nil {
		log.Println("Error open default channel :", err)
		return nil, nil, err
	}

	return conn, defaultChn, nil
}

func (o *RabbitConnection) setupCloseNotify() {

	closeChan := make(chan *amqp.Error)
	o.conn.NotifyClose(closeChan)

	go func() {

		err := <-closeChan

		var sendErr error

		if err != nil {

			sendErr = errors.New(err.Reason)
		}

		for {

			time.Sleep(10 * time.Second)
			log.Println("trying reconnect rabbitmq...")

			conn, chn, err := connect(o.options)

			if err == nil {

				o.conn = conn
				o.defaultChn = chn
				o.setupCloseNotify()

				for _, listener := range o.closeListener {
					listener(sendErr)
				}

				break
			}
		}
	}()
}

func initExchange(chn *amqp.Channel, subject string) error {

	mutex.RLock()
	already := exchangeCreated[subject]
	mutex.RUnlock()

	if !already {

		mutex.Lock()
		defer mutex.Unlock()

		err := chn.ExchangeDeclare(
			subject, // name
			"topic", //kind
			true,    // durable
			false,   // auto delete
			false,   // internal
			false,   // nowait
			nil,
		)

		if err == nil {
			exchangeCreated[subject] = true
		} else {

			log.Println("[ERROR] create exchange error :", err)
		}
	}

	return nil
}

func initQueue(chn *amqp.Channel, queueName string) error {

	mutex.RLock()
	already := queueCreated[queueName]
	mutex.RUnlock()

	if !already {

		mutex.Lock()
		defer mutex.Unlock()

		var err error
		_, err = chn.QueueDeclare(
			queueName,
			true,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			log.Println("[ERROR] create queue error :", err)
			return err
		}

		queueCreated[queueName] = true

		err = chn.Qos(
			5,     // prefetch count
			0,     // prefetch size
			false, // global
		)

		if err != nil {
			log.Println("[WARN] failed set channel QOS :", err)
		}
	}

	return nil
}

func (o *RabbitConnection) Publish(subject, route string, payload string) error {

	initExchange(o.defaultChn, subject)
	o.defaultChn.PublishWithContext(context.Background(),
		subject,
		route,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(payload),
		},
	)
	return nil
}

func (o *RabbitConnection) Subscribe(subject string, routes []string, handler cemirpc.SubscribeHandler, workerNo ...int) error {

	err := o.setupSubscribe(subject, routes, handler, workerNo...)

	if err == nil {

		o.onClose(func(err error) {

			o.setupSubscribe(subject, routes, handler, workerNo...)
		})
	}

	return err
}

func (o *RabbitConnection) setupSubscribe(subject string, routes []string, handler cemirpc.SubscribeHandler, workerNo ...int) error {

	log.Println("Setup Subscribe :", subject, routes)

	chn, err := o.conn.Channel()

	if err != nil {
		return err
	}

	initExchange(chn, subject)

	queue, err := chn.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Println("[ERROR] Declare queue error", err)
		return err
	}

	for _, route := range routes {
		err := chn.QueueBind(queue.Name, route, subject, false, nil)

		if err != nil {
			log.Println("[ERROR] bind error : ", err)
		}
	}

	msgs, err := chn.Consume(queue.Name, "", false, false, false, false, nil)

	if err != nil {
		log.Println("[ERROR] consume error : ", err)
		return err
	}

	go func() {

		realCount := 1

		if len(workerNo) > 0 {
			realCount = workerNo[0]
		}

		if realCount < 0 {
			realCount = 0
		}

		if realCount == 1 {

			for msg := range msgs {

				o.callHandler(msg, subject, handler)
			}
		} else {

			pool := pond.NewPool(realCount, pond.WithQueueSize(realCount))

			for msg := range msgs {

				pool.Submit(func() {

					o.callHandler(msg, subject, handler)
				})
			}
		}
	}()

	return nil
}

func (o *RabbitConnection) callHandler(msg amqp.Delivery, subject string, handler cemirpc.SubscribeHandler) {

	ret := true

	defer func() {

		errCritical := recover()

		if errCritical != nil {
			log.Println("critical error : ", errCritical)
			debug.PrintStack()
			ret = false
		}

		if ret {
			msg.Ack(false)
		} else {
			msg.Nack(false, true)
			time.Sleep(3 * time.Second)
		}
	}()

	ret = handler(subject, string(msg.Body))
}

func (o *RabbitConnection) QueueSubscribe(subject, group string, routes []string, handler cemirpc.SubscribeHandler, workerNo ...int) error {

	err := o.setupQueueSubscribe(subject, group, routes, handler, workerNo...)

	if err == nil {

		o.onClose(func(err error) {

			o.setupQueueSubscribe(subject, group, routes, handler, workerNo...)
		})
	}

	return err
}

func (o *RabbitConnection) setupQueueSubscribe(subject, group string, routes []string, handler cemirpc.SubscribeHandler, workerNo ...int) error {

	log.Println("Setup Queue Subscribe :", subject, group, routes)

	chn, err := o.conn.Channel()

	if err != nil {
		return err
	}

	err = initExchange(chn, subject)
	if err != nil {
		return err
	}

	queueName := subject + "." + group

	err = initQueue(chn, queueName)

	if err != nil {
		return err
	}

	for _, route := range routes {
		err := chn.QueueBind(queueName, route, subject, false, nil)

		if err != nil {
			log.Println("[ERROR] bind error : ", err)
		}
	}

	msgs, err := chn.Consume(queueName, "", false, false, false, false, nil)

	if err != nil {
		return err
	}

	go func() {

		realCount := 1

		if len(workerNo) > 0 {
			realCount = workerNo[0]
		}

		if realCount < 0 {
			realCount = 0
		}

		if realCount == 1 {

			for msg := range msgs {

				o.callHandler(msg, subject, handler)
			}
		} else {

			pool := pond.NewPool(realCount, pond.WithQueueSize(realCount))

			for msg := range msgs {

				pool.Submit(func() {

					o.callHandler(msg, subject, handler)
				})
			}
		}
	}()

	return nil
}

func (o *RabbitConnection) onClose(listner CloseListener) {

	o.closeListener = append(o.closeListener, listner)
}

func (o *RabbitConnection) waitReply(corId string) (ret chan []byte) {

	ret = make(chan []byte)

	o.rpcMutex.Lock()
	o.waitChan[corId] = ret
	o.rpcMutex.Unlock()

	return ret
}

func (o *RabbitConnection) initRPCConsumer() {

	o.rpcOnce.Do(func() {

		o.setupRPCConsumer()

		o.onClose(func(err error) {

			o.setupRPCConsumer()
		})
	})
}

func (o *RabbitConnection) setupRPCConsumer() {

	chn, err := o.conn.Channel()

	if err != nil {
		log.Panic("[PANIC] unable to create rpc channel")
		return
	}

	_, err = chn.QueueDeclare(
		o.rpcQueueName,
		false,
		true,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Panic("[PANIC] unable to create rpc queue")
		return
	}

	msgs, err := chn.Consume(
		o.rpcQueueName, // queue
		"",             // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)

	if err != nil {
		log.Panic("[PANIC] unable to create rpc consumer")
		return
	}

	go func() {

		for msg := range msgs {

			o.rpcMutex.RLock()
			replyChan, exist := o.waitChan[msg.CorrelationId]
			o.rpcMutex.RUnlock()

			if !exist {
				continue
			}

			o.rpcMutex.Lock()
			delete(o.waitChan, msg.CorrelationId)
			o.rpcMutex.Unlock()

			replyChan <- msg.Body
		}
	}()
}

func (o *RabbitConnection) Call(methodName string, params ...any) cemirpc.Result {

	corId := ulid.Make().String()
	paramBytes, err := json.Marshal(params)

	ret := cemirpc.Result{}

	if err != nil {
		log.Println("[ERROR] parsing input param error :", err)

		ret.Error = err
		return ret
	}

	o.initRPCConsumer()

	waitChan := o.waitReply(corId)

	timeout := make(chan string, 1)
	go func() {
		time.Sleep(30 * time.Second)
		timeout <- "timeout"
	}()

	queueName := "RPC." + methodName
	o.defaultChn.Publish(
		"",
		queueName,
		false, // mandatory
		false, // immediate
		amqp091.Publishing{
			ContentType:   "application/json",
			CorrelationId: corId,
			Body:          paramBytes,
			ReplyTo:       o.rpcQueueName,
		})

	var reply []byte

	select {
	case reply = <-waitChan:
		retData := cemirpc.DTO{}
		err = json.Unmarshal(reply, &retData)

		if err != nil {
			ret.Error = err
		} else {
			if retData.Error != "" {
				ret.Error = errors.New(retData.Error)
			} else {
				ret.Data = retData.Data
			}
		}
	case <-timeout:
		ret.Error = errors.New("timeout")
	}

	return ret
}

func (o *RabbitConnection) Serve(methodName string, handler cemirpc.ServeHandler, workerNo ...int) error {

	err := o.setupServe(methodName, handler, workerNo...)

	if err == nil {

		o.onClose(func(err error) {

			o.setupServe(methodName, handler, workerNo...)
		})
	}

	return err
}

func (o *RabbitConnection) setupServe(methodName string, handler cemirpc.ServeHandler, workerNo ...int) error {

	log.Println("Setup RPC serve : ", methodName)
	chn, err := o.conn.Channel()

	if err != nil {
		return err
	}

	queueName := "RPC." + methodName
	initQueue(chn, queueName)

	msgs, err := chn.Consume(queueName, "", false, false, false, false, nil)

	if err != nil {
		return err
	}

	go func() {

		params := []any{}

		maxCon := 1

		if len(workerNo) > 0 {
			maxCon = workerNo[0]
		}

		if maxCon < 0 {
			maxCon = 0
		}

		pool := pond.NewPool(maxCon)

		for msg := range msgs {

			pool.Submit(func() {

				err := json.Unmarshal(msg.Body, &params)

				if err != nil {

					byt, _ := json.Marshal(cemirpc.DTO{
						Error: err.Error(),
					})

					chn.Publish(
						"",
						msg.ReplyTo,
						false,
						false,
						amqp091.Publishing{
							ContentType:   "application/json",
							CorrelationId: msg.CorrelationId,
							Body:          byt,
						},
					)
				}

				retData, err := o.callRPCHandler(methodName, handler, params)

				ret := cemirpc.DTO{}

				if err != nil {
					ret.Error = err.Error()
				} else {

					for _, itm := range retData {

						err, ok := itm.(error)

						if ok {
							ret.Data = append(ret.Data, err.Error())
						} else {
							ret.Data = append(ret.Data, itm)
						}
					}
				}

				byt, _ := json.Marshal(ret)

				chn.Publish(
					"",          // exchange
					msg.ReplyTo, // routing key
					false,       // mandatory
					false,       // immediate
					amqp.Publishing{
						ContentType:   "application/json",
						CorrelationId: msg.CorrelationId,
						Body:          byt,
					})

				msg.Ack(false)
			})
		}
	}()

	return nil
}

func (o *RabbitConnection) callRPCHandler(methodName string, handler cemirpc.ServeHandler, params []any) (ret []any, retErr error) {

	defer func() {

		err := recover()

		if err != nil {
			log.Println("[ERROR] recover from panic when calling rpc handler :", err)
			retErr = errors.New(fmt.Sprint("unable to call handler :", err))
		}
	}()

	return handler(methodName, params...)
}
