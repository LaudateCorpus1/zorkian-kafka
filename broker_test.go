package kafka

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "gopkg.in/check.v1"

	"github.com/discord/zorkian-kafka/proto"
)

var _ = Suite(&BrokerSuite{})

type BrokerSuite struct{}

func (s *BrokerSuite) SetUpTest(c *C) {
	ResetTestLogger(c)
}

// newTestBrokerConf returns BrokerConf with default configuration adjusted for
// tests
func (s *BrokerSuite) newTestBrokerConf(clientID string) BrokerConf {
	conf := NewBrokerConf(clientID)
	conf.ClusterConnectionConf.DialTimeout = 400 * time.Millisecond
	conf.LeaderRetryLimit = 10
	conf.LeaderRetryWait = 2 * time.Millisecond
	return conf
}

type MetadataTester struct {
	host               string
	port               int
	topics             map[string]bool
	allowCreate        bool
	requestDelay       time.Duration
	numGeneralFetches  int
	numSpecificFetches int
}

func NewMetadataHandler(srv *Server, allowCreate bool) *MetadataTester {
	host, port := srv.HostPort()
	tester := &MetadataTester{
		host:        host,
		port:        port,
		allowCreate: allowCreate,
		topics:      make(map[string]bool),
	}
	tester.topics["test"] = true
	return tester
}

func (m *MetadataTester) SetRequestDelay(delay time.Duration) {
	m.requestDelay = delay
}

func (m *MetadataTester) NumGeneralFetches() int {
	return m.numGeneralFetches
}

func (m *MetadataTester) NumSpecificFetches() int {
	return m.numSpecificFetches
}

func (m *MetadataTester) Handler() RequestHandler {
	return func(request Serializable) Serializable {
		time.Sleep(m.requestDelay)

		req := request.(*proto.MetadataReq)

		if len(req.Topics) == 0 {
			m.numGeneralFetches++
		} else {
			m.numSpecificFetches++
		}

		resp := &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: m.host, Port: int32(m.port)},
			},
			Topics: []proto.MetadataRespTopic{},
		}

		wantsTopic := make(map[string]bool)
		for _, topic := range req.Topics {
			if m.allowCreate {
				m.topics[topic] = true
			}
			wantsTopic[topic] = true
		}

		for topic := range m.topics {
			// Return either all topics or only topics that they explicitly requested
			_, explicitTopic := wantsTopic[topic]
			if len(req.Topics) > 0 && !explicitTopic {
				continue
			}

			resp.Topics = append(resp.Topics, proto.MetadataRespTopic{
				Name: topic,
				Partitions: []proto.MetadataRespPartition{
					{
						ID:       0,
						Leader:   1,
						Replicas: []int32{1},
						Isrs:     []int32{1},
					},
					{
						ID:       1,
						Leader:   1,
						Replicas: []int32{1},
						Isrs:     []int32{1},
					},
				},
			})
		}
		return resp
	}
}

func waitForMetadataEpoch(c *C, Prod Producer, epoch int) {
	prod := Prod.(*producer)
	timeout := time.After(time.Minute)
	for {
		e := atomic.LoadInt64(prod.broker.cluster.epoch)
		switch {
		case e == int64(epoch):
			log.Debugf("Metadata epoch reached %d", e)
			return
		case e > int64(epoch):
			// This is a programmer error / mistake in the test logic.
			c.Fatalf("Observed metadata epoch exceeded desired %d > %d", e, epoch)
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-timeout:
			c.Fatalf("Timeout in waitForMetadataEpoch: %d != %d", e, epoch)
		}
	}
}

func (s *BrokerSuite) TestDialWithInvalidAddress(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	addresses := []string{"localhost:4291190", "localhost:2141202", srv.Address()}
	_, err := NewBroker("test-cluster-invalid", addresses, s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)
}

func (s *BrokerSuite) TestDialWithNoAddress(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	_, err := NewBroker("test-cluster-nil-address", nil, s.newTestBrokerConf("tester"))
	c.Assert(err, NotNil)
}

// Ensure that our dial function for the same cluster + client ID results in only one metadata
// request to a server.
func (s *BrokerSuite) TestDialMetadataRequestCached(c *C) {
	InitializeMetadataCache()
	defer uninitializeMetadataCache()

	srv1 := NewServer()
	srv1.Start()
	defer srv1.Close()

	srv2 := NewServer()
	srv2.Start()
	defer srv2.Close()

	srv3 := NewServer()
	srv3.Start()
	defer srv3.Close()

	for i := 0; i < 30; i++ {
		_, err := NewBroker(
			"test-cluster-metadata-cache",
			[]string{srv1.Address(), srv2.Address(), srv3.Address()},
			s.newTestBrokerConf("tester"))
		c.Assert(err, IsNil)
	}

	c.Assert(srv1.Processed+srv2.Processed+srv3.Processed, Equals, 1)
}

func (s *BrokerSuite) TestDialConnectionPoolCached(c *C) {
	InitializeMetadataCache()
	defer uninitializeMetadataCache()

	srv1 := NewServer()
	srv1.Start()
	defer srv1.Close()

	// Same nodes & clientID should share a connection pool.
	brokerSame1, err := NewBroker("test-cluster1",
		[]string{srv1.Address()}, s.newTestBrokerConf("tester1"))
	c.Assert(err, IsNil)
	brokerSame2, err := NewBroker("test-cluster1", []string{srv1.Address()}, s.newTestBrokerConf("tester1"))
	c.Assert(err, IsNil)
	c.Assert(brokerSame1.conns, Equals, brokerSame2.conns)

	// Different nodes & clientID should not share a connection pool.
	brokerDifferent1, err := NewBroker("test-cluster2", []string{srv1.Address()}, s.newTestBrokerConf("tester2"))
	brokerDifferent2, err := NewBroker("test-cluster3", []string{srv1.Address()}, s.newTestBrokerConf("tester3"))
	c.Assert(brokerDifferent1, Not(Equals), brokerDifferent2)
}

// Tests to ensure that our dial function is randomly selecting brokers from the
// list of available brokers
func (s *BrokerSuite) TestDialRandomized(c *C) {
	srv1 := NewServer()
	srv1.Start()
	defer srv1.Close()

	srv2 := NewServer()
	srv2.Start()
	defer srv2.Close()

	srv3 := NewServer()
	srv3.Start()
	defer srv3.Close()

	nodes := []string{srv1.Address(), srv2.Address(), srv3.Address()}
	conf := s.newTestBrokerConf("tester")

	for i := 0; i < 30; i++ {
		_, err := NewCluster(nodes, conf.ClusterConnectionConf)
		c.Assert(err, IsNil)
	}

	c.Assert(srv1.Processed, Not(Equals), 30)
	c.Assert(srv2.Processed, Not(Equals), 30)
	c.Assert(srv3.Processed, Not(Equals), 30)
	c.Assert(srv1.Processed+srv2.Processed+srv3.Processed, Equals, 30)
	c.Assert(srv1.Processed, Not(Equals), 0)
	c.Assert(srv2.Processed, Not(Equals), 0)
	c.Assert(srv3.Processed, Not(Equals), 0)
}

func (s *BrokerSuite) TestProducer(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	broker, err := NewBroker(
		"test-cluster-producer", []string{srv.Address()}, s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	prodConf := NewProducerConf()
	prodConf.RetryWait = time.Millisecond
	producer := broker.Producer(prodConf)
	messages := []*proto.Message{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	_, err = producer.Produce("does-not-exist", 42142, messages...)
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)

	var handleErr error
	var createdMsgs int
	srv.Handle(ProduceRequest, func(request Serializable) Serializable {
		req := request.(*proto.ProduceReq)
		if req.Topics[0].Name != "test" {
			handleErr = fmt.Errorf("expected 'test' topic, got %s", req.Topics[0].Name)
			return nil
		}
		if req.Topics[0].Partitions[0].ID != 0 {
			handleErr = fmt.Errorf("expected 0 partition, got %d", req.Topics[0].Partitions[0].ID)
			return nil
		}
		messages := req.Topics[0].Partitions[0].Messages
		for _, msg := range messages {
			createdMsgs++
			crc := proto.ComputeCrc(msg, proto.CompressionNone)
			if msg.Crc != crc {
				handleErr = fmt.Errorf("expected '%d' crc, got %d", crc, msg.Crc)
				return nil
			}
		}
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						{
							ID:     0,
							Offset: 5,
						},
					},
				},
			},
		}
	})

	offset, err := producer.Produce("test", 0, messages...)
	c.Assert(handleErr, IsNil)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(5))
	c.Assert(createdMsgs, Equals, 2)
	if messages[0].Offset != 5 || messages[1].Offset != 6 {
		c.Fatalf("message offset is incorrect: %#v", messages)
	}
}

func (s *BrokerSuite) TestProducerWithNoAck(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	broker, err := NewBroker(
		"test-cluster-no-ack", []string{srv.Address()}, s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	prodConf := NewProducerConf()
	prodConf.RequiredAcks = proto.RequiredAcksNone
	prodConf.RetryWait = time.Millisecond
	producer := broker.Producer(prodConf)
	messages := []*proto.Message{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	_, err = producer.Produce("does-not-exist", 42142, messages...)
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)

	errc := make(chan error)
	var createdMsgs int
	srv.Handle(ProduceRequest, func(request Serializable) Serializable {
		defer close(errc)
		req := request.(*proto.ProduceReq)
		if req.RequiredAcks != proto.RequiredAcksNone {
			errc <- fmt.Errorf("expected no ack request, got %v", req.RequiredAcks)
			return nil
		}
		if req.Topics[0].Name != "test" {
			errc <- fmt.Errorf("expected 'test' topic, got %s", req.Topics[0].Name)
			return nil
		}
		if req.Topics[0].Partitions[0].ID != 0 {
			errc <- fmt.Errorf("expected 0 partition, got %d", req.Topics[0].Partitions[0].ID)
			return nil
		}
		messages := req.Topics[0].Partitions[0].Messages
		for _, msg := range messages {
			createdMsgs++
			crc := proto.ComputeCrc(msg, proto.CompressionNone)
			if msg.Crc != crc {
				errc <- fmt.Errorf("expected '%d' crc, got %d", crc, msg.Crc)
				return nil
			}
		}
		return nil
	})

	offset, err := producer.Produce("test", 0, messages...)
	if err := <-errc; err != nil {
		c.Fatalf("handling error: %s", err)
	}
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(0))
	c.Assert(createdMsgs, Equals, 2)

}

func (s *BrokerSuite) TestMetadataRefreshSerialization(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	mdh := NewMetadataHandler(srv, false)
	mdh.SetRequestDelay(100 * time.Millisecond)
	srv.Handle(MetadataRequest, mdh.Handler())

	broker, err := NewBroker(
		"test-cluster-metadata-refresh",
		[]string{srv.Address()},
		s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	c.Assert(atomic.LoadInt64(broker.cluster.epoch), Equals, int64(1))
	c.Assert(broker.cluster.RefreshMetadata(), IsNil)
	c.Assert(atomic.LoadInt64(broker.cluster.epoch), Equals, int64(2))

	// Now try two at once, assert only one increment
	go broker.cluster.RefreshMetadata()
	c.Assert(broker.cluster.RefreshMetadata(), IsNil)
	c.Assert(atomic.LoadInt64(broker.cluster.epoch), Equals, int64(3))

	// Now test that the timeout works
	func() {
		broker.cluster.mu.Lock()
		defer broker.cluster.mu.Unlock()
		broker.cluster.timeout = 1 * time.Millisecond
	}()
	c.Assert(broker.cluster.RefreshMetadata(), NotNil)
}

func (s *BrokerSuite) TestProduceWhileLeaderChange(c *C) {
	srv1 := NewServer()
	srv1.Start()
	defer srv1.Close()

	srv2 := NewServer()
	srv2.Start()
	defer srv2.Close()

	host1, port1 := srv1.HostPort()
	host2, port2 := srv2.HostPort()
	brokers := []proto.MetadataRespBroker{
		{NodeID: 1, Host: host1, Port: int32(port1)},
		{NodeID: 2, Host: host2, Port: int32(port2)},
	}

	// send invalid information to producer several times to make sure
	// it's producing to the wrong node and retrying several times
	numTriesRequired := 4

	var metaCalls int
	metadataHandler := func(srvName string) func(Serializable) Serializable {
		return func(request Serializable) Serializable {
			metaCalls++

			var leader int32 = 1
			// send invalid information to producer several times to make sure
			// it's producing to the wrong node and retrying several times
			if metaCalls > numTriesRequired {
				leader = 2
			}
			req := request.(*proto.MetadataReq)
			resp := &proto.MetadataResp{
				CorrelationID: req.CorrelationID,
				Brokers:       brokers,
				Topics: []proto.MetadataRespTopic{
					{
						Name: "test",
						Partitions: []proto.MetadataRespPartition{
							{
								ID:       0,
								Leader:   1,
								Replicas: []int32{1, 2},
								Isrs:     []int32{1, 2},
							},
							{
								ID:       1,
								Leader:   leader,
								Replicas: []int32{1, 2},
								Isrs:     []int32{1, 2},
							},
						},
					},
				},
			}
			return resp
		}
	}

	srv1.Handle(MetadataRequest, metadataHandler("srv1"))
	srv2.Handle(MetadataRequest, metadataHandler("srv2"))

	var prod1Calls int
	srv1.Handle(ProduceRequest, func(request Serializable) Serializable {
		prod1Calls++
		req := request.(*proto.ProduceReq)
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						{
							ID:  1,
							Err: proto.ErrNotLeaderForPartition,
						},
					},
				},
			},
		}
	})

	var prod2Calls int
	srv2.Handle(ProduceRequest, func(request Serializable) Serializable {
		prod2Calls++
		req := request.(*proto.ProduceReq)
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						{
							ID:     1,
							Offset: 5,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-leader-change",
		[]string{srv1.Address()},
		s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	prod := broker.Producer(NewProducerConf())
	for try := 0; try < numTriesRequired; try++ {
		waitForMetadataEpoch(c, prod, try+1)
		_, err := prod.Produce(
			"test", 1, &proto.Message{Value: []byte("foo")})
		c.Assert(err, Not(IsNil))
	}
	waitForMetadataEpoch(c, prod, numTriesRequired+1)
	off, err := prod.Produce(
		"test", 1, &proto.Message{Value: []byte("foo")})
	c.Assert(err, IsNil)
	c.Assert(off, Equals, int64(5))
	c.Assert(prod1Calls, Equals, numTriesRequired)
	c.Assert(prod2Calls, Equals, 1)
}

func (s *BrokerSuite) TestConsumer(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		host, port := srv.HostPort()
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       413,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	})
	fetchCallCount := 0
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		fetchCallCount++
		if fetchCallCount < 2 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:        413,
								TipOffset: 0,
								Messages:  []*proto.Message{},
							},
						},
					},
				},
			}
		}

		messages := []*proto.Message{
			{Offset: 3, Key: []byte("1"), Value: []byte("first")},
			{Offset: 4, Key: []byte("2"), Value: []byte("second")},
			{Offset: 5, Key: []byte("3"), Value: []byte("third")},
		}

		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        413,
							TipOffset: 2,
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-consumer", []string{srv.Address()}, s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	_, err = broker.Consumer(NewConsumerConf("does-not-exists", 413))
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)

	_, err = broker.Consumer(NewConsumerConf("test", 1))
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)

	consConf := NewConsumerConf("test", 413)
	consConf.RetryWait = time.Millisecond
	consConf.StartOffset = 0
	consConf.RetryLimit = 4
	consumer, err := broker.Consumer(consConf)
	c.Assert(err, IsNil)

	msg1, err := consumer.Consume()
	c.Assert(err, IsNil)
	if string(msg1.Value) != "first" || string(msg1.Key) != "1" || msg1.Offset != 3 {
		c.Fatalf("expected different message than %#v", msg1)
	}

	msg2, err := consumer.Consume()
	c.Assert(err, IsNil)
	if string(msg2.Value) != "second" || string(msg2.Key) != "2" || msg2.Offset != 4 {
		c.Fatalf("expected different message than %#v", msg2)
	}
}

func (s *BrokerSuite) TestBatchConsumer(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		host, port := srv.HostPort()
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       413,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	})
	fetchCallCount := 0
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		fetchCallCount++
		if fetchCallCount < 2 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:        413,
								TipOffset: 0,
								Messages:  []*proto.Message{},
							},
						},
					},
				},
			}
		}

		messages := []*proto.Message{
			{Offset: 3, Key: []byte("1"), Value: []byte("first")},
			{Offset: 4, Key: []byte("2"), Value: []byte("second")},
			{Offset: 5, Key: []byte("3"), Value: []byte("third")},
		}

		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        413,
							TipOffset: 2,
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-batch-consumer",
		[]string{srv.Address()},
		s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	_, err = broker.BatchConsumer(NewConsumerConf("does-not-exists", 413))
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)

	_, err = broker.BatchConsumer(NewConsumerConf("test", 1))
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)

	consConf := NewConsumerConf("test", 413)
	consConf.RetryWait = time.Millisecond
	consConf.StartOffset = 0
	consConf.RetryLimit = 4
	consumer, err := broker.BatchConsumer(consConf)
	c.Assert(err, IsNil)

	batch, err := consumer.ConsumeBatch()
	c.Assert(err, IsNil)
	c.Assert(len(batch), Equals, 3)

	if string(batch[0].Value) != "first" || string(batch[0].Key) != "1" || batch[0].Offset != 3 {
		c.Fatalf("expected different message than %#v", batch[0])
	}

	if string(batch[1].Value) != "second" || string(batch[1].Key) != "2" || batch[1].Offset != 4 {
		c.Fatalf("expected different message than %#v", batch[1])
	}

	if string(batch[2].Value) != "third" || string(batch[2].Key) != "3" || batch[2].Offset != 5 {
		c.Fatalf("expected different message than %#v", batch[2])
	}
}

func (s *BrokerSuite) TestConsumerRetry(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		host, port := srv.HostPort()
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	})
	fetchCallCount := 0
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		fetchCallCount++
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        0,
							TipOffset: 0,
							Messages:  []*proto.Message{},
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-retry", []string{srv.Address()}, s.newTestBrokerConf("test"))
	c.Assert(err, IsNil)

	consConf := NewConsumerConf("test", 0)
	consConf.RetryLimit = 5
	consConf.StartOffset = 0
	consConf.RetryWait = time.Millisecond
	consumer, err := broker.Consumer(consConf)
	c.Assert(err, IsNil)

	_, err = consumer.Consume()
	c.Assert(err, Equals, ErrNoData)
	c.Assert(fetchCallCount, Equals, 6)
}

func (s *BrokerSuite) TestConsumeInvalidOffset(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		messages := []*proto.Message{
			// return message with offset lower than requested
			{Offset: 3, Key: []byte("1"), Value: []byte("first")},
			{Offset: 4, Key: []byte("2"), Value: []byte("second")},
			{Offset: 5, Key: []byte("3"), Value: []byte("third")},
		}
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        0,
							TipOffset: 2,
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-invalid-offset",
		[]string{srv.Address()},
		s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	consConf := NewConsumerConf("test", 0)
	consConf.StartOffset = 4
	consumer, err := broker.Consumer(consConf)
	c.Assert(err, IsNil)

	msg, err := consumer.Consume()
	c.Assert(err, IsNil)
	if string(msg.Value) != "second" || string(msg.Key) != "2" || msg.Offset != 4 {
		c.Fatalf("expected different message than %#v", msg)
	}
}

func (s *BrokerSuite) TestPartitionOffset(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	md := NewMetadataHandler(srv, false)
	srv.Handle(MetadataRequest, md.Handler())

	count := 0
	var handlerErr error
	srv.Handle(OffsetRequest, func(request Serializable) Serializable {
		req := request.(*proto.OffsetReq)
		if req.ReplicaID != -1 {
			handlerErr = fmt.Errorf("expected -1 replica id, got %d", req.ReplicaID)
		}
		if req.Topics[0].Partitions[0].TimeMs != -2 {
			handlerErr = fmt.Errorf("expected -2 timems, got %d", req.Topics[0].Partitions[0].TimeMs)
		}

		var resErr error
		count++
		if count < 3 {
			resErr = proto.ErrLeaderNotAvailable
		}

		return &proto.OffsetResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.OffsetRespTopic{
				{
					Name: "test",
					Partitions: []proto.OffsetRespPartition{
						{
							ID:      1,
							Offsets: []int64{123, 0},
							Err:     resErr,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-partition-offset",
		[]string{srv.Address()},
		s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	c.Assert(md.NumGeneralFetches(), Equals, 1)
	offset, err := broker.offset("test", 1, -2)
	c.Assert(handlerErr, IsNil)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(123))
	c.Assert(md.NumGeneralFetches(), Equals, 3)
}

func (s *BrokerSuite) TestPartitionCount(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	broker, err := NewBroker(
		"test-cluster-partition-count",
		[]string{srv.Address()},
		s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	count, err := broker.cluster.PartitionCount("test")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int32(2))

	count, err = broker.cluster.PartitionCount("test2")
	c.Assert(err, NotNil)
	c.Assert(count, Equals, int32(0))
}

func (s *BrokerSuite) TestPartitionOffsetClosedConnection(c *C) {
	srv1 := NewServer()
	srv1.Start()
	srv2 := NewServer()
	srv2.Start()

	host1, port1 := srv1.HostPort()
	host2, port2 := srv2.HostPort()

	var handlerErr error
	srv1.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host1, Port: int32(port1)},
				{NodeID: 2, Host: host2, Port: int32(port2)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   1,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
						{
							ID:       1,
							Leader:   1,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
					},
				},
			},
		}
	})
	srv1.Handle(OffsetRequest, func(request Serializable) Serializable {
		req := request.(*proto.OffsetReq)
		if req.ReplicaID != -1 {
			handlerErr = fmt.Errorf("expected -1 replica id, got %d", req.ReplicaID)
		}
		if req.Topics[0].Partitions[0].TimeMs != -2 {
			handlerErr = fmt.Errorf("expected -2 timems, got %d", req.Topics[0].Partitions[0].TimeMs)
		}
		return &proto.OffsetResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.OffsetRespTopic{
				{
					Name: "test",
					Partitions: []proto.OffsetRespPartition{
						{
							ID:      1,
							Offsets: []int64{123, 0},
						},
					},
				},
			},
		}
	})
	// after closing first server, which started as leader, broker should ask
	// other nodes about the leader and refresh connections
	srv2.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host1, Port: int32(port1)},
				{NodeID: 2, Host: host2, Port: int32(port2)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   2,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
						{
							ID:       1,
							Leader:   2,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
					},
				},
			},
		}
	})
	srv2.Handle(OffsetRequest, func(request Serializable) Serializable {
		req := request.(*proto.OffsetReq)
		if req.ReplicaID != -1 {
			handlerErr = fmt.Errorf("expected -1 replica id, got %d", req.ReplicaID)
		}
		if req.Topics[0].Partitions[0].TimeMs != -2 {
			handlerErr = fmt.Errorf("expected -2 timems, got %d", req.Topics[0].Partitions[0].TimeMs)
		}
		return &proto.OffsetResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.OffsetRespTopic{
				{
					Name: "test",
					Partitions: []proto.OffsetRespPartition{
						{
							ID:      1,
							Offsets: []int64{234, 0},
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-closed-conn", []string{srv1.Address()}, s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	offset, err := broker.offset("test", 1, -2)
	c.Assert(handlerErr, IsNil)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(123))
	c.Assert(len(broker.conns.backends), Equals, 2)

	srv1.Close()

	// This test is fundamentally broken. The above calls conn.Close on the client,
	// but then it's a race. If the kernel happens to pass around the RST/close
	// quickly, then the connection pool will never return the dead connection.
	// However, if things are slow, we will get a dead connection.
	//
	// So, we now test two possibilities: either we get a dead connection, and
	// then subsequently it works, or we get a valid connection and it works the
	// first time.

	offset, err = broker.offset("test", 1, -2)
	if err != nil {
		// First request failed, validate it failed correctly.
		c.Assert(offset, Equals, int64(0))
		c.Assert(handlerErr, IsNil)
		c.Assert(err, NotNil)

		// Do second request, since first failed.
		offset, err = broker.offset("test", 1, -2)
	}

	// Now validate either the second request or the successful first request.
	c.Assert(handlerErr, IsNil)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(234))

	srv2.Close()
}

func (s *BrokerSuite) TestConsumeWhilePartitionMoves(c *C) {
	srv1 := NewServer()
	srv1.Start()
	defer srv1.Close()

	srv2 := NewServer()
	srv2.Start()
	defer srv2.Close()

	srv3 := NewServer()
	srv3.Start()
	defer srv3.Close()

	host1, port1 := srv1.HostPort()
	host2, port2 := srv2.HostPort()
	host3, port3 := srv3.HostPort()
	brokers := []proto.MetadataRespBroker{
		{NodeID: 1, Host: host1, Port: int32(port1)},
		{NodeID: 2, Host: host2, Port: int32(port2)},
		{NodeID: 3, Host: host3, Port: int32(port3)},
	}

	var metaCalls int
	metadataHandler := func(srvName string) func(Serializable) Serializable {
		return func(request Serializable) Serializable {
			metaCalls++

			// send invalid information to producer several times to make sure
			// client is consuming wrong node and retrying several times before
			// succeeding
			var state []proto.MetadataRespPartition
			if metaCalls < 3 {
				state = []proto.MetadataRespPartition{
					{
						ID:       0,
						Leader:   1,
						Replicas: []int32{1, 2},
						Isrs:     []int32{1, 2},
					},
					{
						ID:       1,
						Leader:   1,
						Replicas: []int32{1, 2},
						Isrs:     []int32{1, 2},
					},
				}
			} else if metaCalls < 6 {
				state = []proto.MetadataRespPartition{
					{
						ID:       0,
						Leader:   1,
						Replicas: []int32{1, 2},
						Isrs:     []int32{1, 2},
					},
					{
						ID:       1,
						Leader:   -1,
						Replicas: []int32{1, 2},
						Isrs:     []int32{1, 2},
					},
				}
			} else {
				state = []proto.MetadataRespPartition{
					{
						ID:       0,
						Leader:   1,
						Replicas: []int32{1, 2},
						Isrs:     []int32{1, 2},
					},
					{
						ID:       1,
						Leader:   3,
						Replicas: []int32{3, 2},
						Isrs:     []int32{3, 2},
					},
				}
			}
			req := request.(*proto.MetadataReq)
			resp := &proto.MetadataResp{
				CorrelationID: req.CorrelationID,
				Brokers:       brokers,
				Topics: []proto.MetadataRespTopic{
					{
						Name:       "test",
						Partitions: state,
					},
				},
			}
			return resp
		}
	}

	srv1.Handle(MetadataRequest, metadataHandler("srv1"))
	srv2.Handle(MetadataRequest, metadataHandler("srv2"))
	srv3.Handle(MetadataRequest, metadataHandler("srv3"))

	var fetch1Calls int
	srv1.Handle(FetchRequest, func(request Serializable) Serializable {
		fetch1Calls++
		req := request.(*proto.FetchReq)

		if fetch1Calls == 1 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:        1,
								TipOffset: 4,
								Messages: []*proto.Message{
									{Offset: 1, Value: []byte("first")},
								},
							},
						},
					},
				},
			}
		}

		// partition is no longer ours
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:  1,
							Err: proto.ErrUnknownTopicOrPartition,
						},
					},
				},
			},
		}
	})

	var fetch2Calls int
	srv2.Handle(FetchRequest, func(request Serializable) Serializable {
		fetch2Calls++
		req := request.(*proto.FetchReq)
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:  1,
							Err: proto.ErrNotLeaderForPartition,
						},
					},
				},
			},
		}
	})

	var fetch3Calls int
	srv3.Handle(FetchRequest, func(request Serializable) Serializable {
		fetch3Calls++
		req := request.(*proto.FetchReq)

		if metaCalls < 6 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:  1,
								Err: proto.ErrUnknownTopicOrPartition,
							},
						},
					},
				},
			}
		}

		// Enough meta calls, we're now known
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        1,
							TipOffset: 8,
							Messages: []*proto.Message{
								{Offset: 2, Value: []byte("second")},
							},
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-partition-moves",
		[]string{srv1.Address()},
		s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	conf := NewConsumerConf("test", 1)
	conf.StartOffset = 0
	cons, err := broker.Consumer(conf)
	c.Assert(err, IsNil)

	// consume twice - once from srv1 and once from srv3
	// srv2 is never consumed even though it's in the partition set, because it was
	// never the leader.
	m, err := cons.Consume()
	c.Assert(err, IsNil)
	c.Assert(m.Offset, Equals, int64(1))

	m, err = cons.Consume()
	c.Assert(err, IsNil)
	c.Assert(m.Offset, Equals, int64(2))

	// 1,2,3   -> srv1
	// 4, 5    -> no leader
	// 6, 7... -> srv3
	c.Assert(metaCalls, Equals, 6)
	c.Assert(fetch1Calls, Equals, 3)
	c.Assert(fetch2Calls, Equals, 0)
	c.Assert(fetch3Calls, Equals, 1)
}

func (s *BrokerSuite) TestConsumerSeekToLatest(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		host, port := srv.HostPort()
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       413,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	})
	fetchCallCount := 0
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		fetchCallCount++
		if fetchCallCount < 2 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:        413,
								TipOffset: 0,
								Messages:  []*proto.Message{},
							},
						},
					},
				},
			}
		}

		messages := []*proto.Message{
			{Offset: 3, Key: []byte("1"), Value: []byte("first")},
			{Offset: 4, Key: []byte("2"), Value: []byte("second")},
			{Offset: 5, Key: []byte("3"), Value: []byte("third")},
		}

		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        413,
							TipOffset: 2,
							Messages:  messages,
						},
					},
				},
			},
		}
	})
	srv.Handle(OffsetRequest, func(request Serializable) Serializable {
		req := request.(*proto.OffsetReq)
		return &proto.OffsetResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.OffsetRespTopic{
				{
					Name: "test",
					Partitions: []proto.OffsetRespPartition{
						{
							ID:      413,
							Offsets: []int64{5, 0},
							Err:     nil,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-seektolatest-broker", []string{srv.Address()}, s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	consConf := NewConsumerConf("test", 413)
	consConf.RetryWait = time.Millisecond
	consConf.StartOffset = 0
	consConf.RetryLimit = 4
	consumer, err := broker.Consumer(consConf)
	c.Assert(err, IsNil)

	msg1, err := consumer.Consume()
	c.Assert(err, IsNil)
	if string(msg1.Value) != "first" || string(msg1.Key) != "1" || msg1.Offset != 3 {
		c.Fatalf("expected different message than %#v", msg1)
	}

	err = consumer.SeekToLatest()
	c.Assert(err, IsNil)

	msg2, err := consumer.Consume()
	c.Assert(err, IsNil)
	if string(msg2.Value) != "third" || string(msg2.Key) != "3" || msg2.Offset != 5 {
		c.Fatalf("expected different message than %#v", msg2)
	}
}

func (s *BrokerSuite) TestLeaderConnectionFailover(c *C) {
	c.Skip("bad test, needs to be rewritten")

	srv1 := NewServer()
	srv1.Start()

	srv2 := NewServer()
	srv2.Start()
	defer srv2.Close()

	host1, port1 := srv1.HostPort()
	host2, port2 := srv2.HostPort()

	resps := []*proto.MetadataResp{
		&proto.MetadataResp{
			CorrelationID: 0,
			Brokers: []proto.MetadataRespBroker{
				{
					NodeID: 1,
					Host:   host1,
					Port:   int32(port1),
				},
				{
					NodeID: 2,
					Host:   host2,
					Port:   int32(port2),
				},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		},
		&proto.MetadataResp{
			CorrelationID: 0,
			Brokers: []proto.MetadataRespBroker{
				{
					NodeID: 1,
					Host:   host1,
					Port:   int32(port1),
				},
				{
					NodeID: 2,
					Host:   host2,
					Port:   int32(port2),
				},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		},
		&proto.MetadataResp{
			CorrelationID: 0,
			Brokers: []proto.MetadataRespBroker{
				{
					NodeID: 1,
					Host:   host1,
					Port:   int32(port1),
				},
				{
					NodeID: 2,
					Host:   host2,
					Port:   int32(port2),
				},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   2,
							Replicas: []int32{2},
							Isrs:     []int32{2},
						},
					},
				},
			},
		},
	}
	handler := func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		resp := resps[0]
		resp.CorrelationID = req.CorrelationID
		resps = resps[1:]
		return resp
	}

	srv1.Handle(MetadataRequest, handler)
	srv2.Handle(MetadataRequest, handler)

	conf := s.newTestBrokerConf("tester")
	conf.ClusterConnectionConf.DialTimeout = time.Millisecond * 50
	conf.LeaderRetryWait = time.Millisecond
	conf.LeaderRetryLimit = 3

	// Force connection to first broker to start with
	broker, err := NewBroker("test-cluster-leader-failover", []string{srv1.Address()}, conf)
	c.Assert(broker, NotNil)
	c.Assert(err, IsNil)

	_, err = broker.leaderConnection("does-not-exist", 123456)
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)

	conn, err := broker.leaderConnection("test", 0)
	c.Assert(conn, NotNil)
	c.Assert(err, IsNil)

	tp := topicPartition{"test", 0}
	nodeID, ok := broker.cluster.endpoints[tp]
	c.Assert(ok, Equals, true)
	c.Assert(nodeID, Equals, int32(1))

	// This is again a racy condition. We need to give a little time for all
	// of the connections to close.
	srv1.Close()
	time.Sleep(500 * time.Millisecond)

	_, err = broker.leaderConnection("test", 0)
	c.Assert(err, NotNil)

	// provide node address that will be available after short period
	broker.cluster.nodes = map[int32]string{
		2: srv2.Address(),
	}

	// Add server2 to the connection map (dirty hack to make this test)
	// work, else we might have gotten metadata from node2 to begin with
	broker.conns.InitializeAddrs([]string{srv2.Address()})

	_, err = broker.leaderConnection("test", 0)
	c.Assert(err, IsNil)

	nodeID, ok = broker.cluster.endpoints[tp]
	c.Assert(ok, Equals, true)
	c.Assert(nodeID, Equals, int32(2))
}

func (s *BrokerSuite) TestProducerFailoverRequestTimeout(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	requestsCount := 0
	srv.Handle(ProduceRequest, func(request Serializable) Serializable {
		req := request.(*proto.ProduceReq)
		requestsCount++
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						{
							ID:  0,
							Err: proto.ErrRequestTimeout,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-produce-timeout",
		[]string{srv.Address()},
		s.newTestBrokerConf("test"))
	c.Assert(err, IsNil)

	prodConf := NewProducerConf()
	prodConf.RetryLimit = 4
	prodConf.RetryWait = time.Millisecond
	producer := broker.Producer(prodConf)

	_, err = producer.Produce(
		"test", 0,
		&proto.Message{Value: []byte("first")},
		&proto.Message{Value: []byte("second")})
	c.Assert(err, Equals, proto.ErrRequestTimeout)
	c.Assert(requestsCount, Equals, 1)
}

func (s *BrokerSuite) TestProducerFailoverLeaderNotAvailable(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	// send invalid information to producer several times to make sure
	// it's producing to the wrong node and retrying several times
	numTriesRequired := 4

	requestsCount := 0
	srv.Handle(ProduceRequest, func(request Serializable) Serializable {
		req := request.(*proto.ProduceReq)
		requestsCount++

		if requestsCount > numTriesRequired {
			return &proto.ProduceResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.ProduceRespTopic{
					{
						Name: "test",
						Partitions: []proto.ProduceRespPartition{
							{
								ID:     0,
								Offset: 11,
							},
						},
					},
				},
			}
		}

		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						{
							ID:  0,
							Err: proto.ErrLeaderNotAvailable,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-leader-unavailable",
		[]string{srv.Address()},
		s.newTestBrokerConf("test"))
	c.Assert(err, IsNil)

	prodConf := NewProducerConf()
	prodConf.RetryLimit = 5
	prodConf.RetryWait = time.Millisecond
	producer := broker.Producer(prodConf)

	for try := 0; try < numTriesRequired; try++ {
		waitForMetadataEpoch(c, producer, try+1)
		_, err = producer.Produce(
			"test", 0,
			&proto.Message{Value: []byte("first")},
			&proto.Message{Value: []byte("second")})
		c.Assert(err, Not(IsNil))
	}
	waitForMetadataEpoch(c, producer, numTriesRequired+1)

	_, err = producer.Produce(
		"test", 0,
		&proto.Message{Value: []byte("first")},
		&proto.Message{Value: []byte("second")})
	c.Assert(err, IsNil)
	c.Assert(requestsCount, Equals, numTriesRequired+1)
}

func (s *BrokerSuite) TestProducerNoCreateTopic(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	md := NewMetadataHandler(srv, false)
	srv.Handle(MetadataRequest, md.Handler())

	produces := 0
	srv.Handle(ProduceRequest,
		func(request Serializable) Serializable {
			produces++

			// Must return something?
			req := request.(*proto.ProduceReq)
			return &proto.ProduceResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.ProduceRespTopic{
					{
						Name: "test2",
						Partitions: []proto.ProduceRespPartition{
							{
								ID:     0,
								Offset: 5,
							},
						},
					},
				},
			}
		},
	)

	// Broker DO NOT create topic
	brokerConf := s.newTestBrokerConf("test")
	brokerConf.AllowTopicCreation = false

	broker, err := NewBroker("test-cluster-no-create-topic", []string{srv.Address()}, brokerConf)
	c.Assert(err, IsNil)

	prodConf := NewProducerConf()
	prodConf.RetryLimit = 5
	prodConf.RetryWait = time.Millisecond
	producer := broker.Producer(prodConf)

	_, err = producer.Produce(
		"test2", 0,
		&proto.Message{Value: []byte("first")},
		&proto.Message{Value: []byte("second")})
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)
	c.Assert(md.NumSpecificFetches(), Equals, 0)
	c.Assert(produces, Equals, 0)
}

func (s *BrokerSuite) TestProducerTryCreateTopic(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	md := NewMetadataHandler(srv, true)
	srv.Handle(MetadataRequest, md.Handler())

	produces := 0
	srv.Handle(ProduceRequest,
		func(request Serializable) Serializable {
			produces++

			// Must return something?
			req := request.(*proto.ProduceReq)
			return &proto.ProduceResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.ProduceRespTopic{
					{
						Name: "test2",
						Partitions: []proto.ProduceRespPartition{
							{
								ID:     0,
								Offset: 5,
							},
						},
					},
				},
			}
		},
	)

	// Broker DO create topic
	brokerConf := s.newTestBrokerConf("test")
	brokerConf.AllowTopicCreation = true

	broker, err := NewBroker("test-cluster-try-create-topic", []string{srv.Address()}, brokerConf)
	c.Assert(err, IsNil)

	prodConf := NewProducerConf()
	prodConf.RetryLimit = 5
	prodConf.RetryWait = time.Millisecond
	producer := broker.Producer(prodConf)

	_, err = producer.Produce("test2", 0, &proto.Message{Value: []byte("first")},
		&proto.Message{Value: []byte("second")})
	c.Assert(err, IsNil)
	c.Assert(md.NumSpecificFetches(), Equals, 1)
	c.Assert(produces, Equals, 1)
}

func (s *BrokerSuite) TestConsumeWhileLeaderChange(c *C) {
	srv1 := NewServer()
	srv1.Start()
	defer srv1.Close()

	srv2 := NewServer()
	srv2.Start()
	defer srv2.Close()

	host1, port1 := srv1.HostPort()
	host2, port2 := srv2.HostPort()
	brokers := []proto.MetadataRespBroker{
		{NodeID: 1, Host: host1, Port: int32(port1)},
		{NodeID: 2, Host: host2, Port: int32(port2)},
	}

	var metaCalls int
	metadataHandler := func(srvName string) func(Serializable) Serializable {
		return func(request Serializable) Serializable {
			metaCalls++

			var leader int32
			// send invalid information to producer several times to make sure
			// client is consuming wrong node and retrying several times before
			// succeeding
			if metaCalls < 3 {
				leader = 1
			} else if metaCalls < 6 {
				leader = 0
			} else {
				leader = 2
			}
			req := request.(*proto.MetadataReq)
			resp := &proto.MetadataResp{
				CorrelationID: req.CorrelationID,
				Brokers:       brokers,
				Topics: []proto.MetadataRespTopic{
					{
						Name: "test",
						Partitions: []proto.MetadataRespPartition{
							{
								ID:       0,
								Leader:   1,
								Replicas: []int32{1, 2},
								Isrs:     []int32{1, 2},
							},
							{
								ID:       1,
								Leader:   leader,
								Replicas: []int32{1, 2},
								Isrs:     []int32{1, 2},
							},
						},
					},
				},
			}
			return resp
		}
	}

	srv1.Handle(MetadataRequest, metadataHandler("srv1"))
	srv2.Handle(MetadataRequest, metadataHandler("srv2"))

	var fetch1Calls int
	srv1.Handle(FetchRequest, func(request Serializable) Serializable {
		fetch1Calls++
		req := request.(*proto.FetchReq)

		if fetch1Calls == 1 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:        1,
								TipOffset: 4,
								Messages: []*proto.Message{
									{Offset: 1, Value: []byte("first")},
								},
							},
						},
					},
				},
			}
		}
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:  1,
							Err: proto.ErrNotLeaderForPartition,
						},
					},
				},
			},
		}
	})

	var fetch2Calls int
	srv2.Handle(FetchRequest, func(request Serializable) Serializable {
		fetch2Calls++
		req := request.(*proto.FetchReq)
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        1,
							TipOffset: 8,
							Messages: []*proto.Message{
								{Offset: 2, Value: []byte("second")},
							},
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-consume-leader-change",
		[]string{srv1.Address()},
		s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	conf := NewConsumerConf("test", 1)
	conf.StartOffset = 0
	cons, err := broker.Consumer(conf)
	c.Assert(err, IsNil)

	// consume twice - once from srv1 and once from srv2
	m, err := cons.Consume()
	c.Assert(err, IsNil)
	c.Assert(m.Offset, Equals, int64(1))

	m, err = cons.Consume()
	c.Assert(err, IsNil)
	c.Assert(m.Offset, Equals, int64(2))

	// 1,2,3   -> srv1
	// 4, 5    -> no leader
	// 6, 7... -> srv2
	c.Assert(metaCalls, Equals, 6)
	c.Assert(fetch1Calls, Equals, 3)
	c.Assert(fetch2Calls, Equals, 1)
}

func (s *BrokerSuite) TestConsumerFailover(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	messages := []*proto.Message{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	respCount := 0
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		respCount++
		req := request.(*proto.FetchReq)

		if respCount == 4 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:  1,
								Err: nil,
							},
						},
					},
				},
			}
		}
		resp := &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:       1,
							Messages: messages,
						},
					},
				},
			},
		}
		messages = []*proto.Message{}
		return resp
	})

	broker, err := NewBroker(
		"test-cluster-consumer-failover",
		[]string{srv.Address()},
		s.newTestBrokerConf("test"))
	c.Assert(err, IsNil)

	conf := NewConsumerConf("test", 1)
	conf.RetryWait = time.Nanosecond
	conf.RetryLimit = 4
	conf.RetryErrWait = time.Nanosecond
	conf.StartOffset = 0

	consumer, err := broker.Consumer(conf)
	c.Assert(err, IsNil)

	for {
		msg, err := consumer.Consume()
		c.Assert(err, IsNil)
		c.Assert(string(msg.Value), Equals, "first")

		msg, err = consumer.Consume()
		c.Assert(err, IsNil)
		c.Assert(string(msg.Value), Equals, "second")

		if msg, err = consumer.Consume(); err != ErrNoData {
			c.Fatalf("expected no data, got %#v (%#q)", err, msg)
		}

		return
	}
}

func (s *BrokerSuite) TestProducerBrokenPipe(c *C) {
	c.Skip("bad test, needs to be rewritten")
	srv1 := NewServer()
	srv1.Start()
	srv2 := NewServer()
	srv2.Start()

	host1, port1 := srv1.HostPort()
	log.Infof("server1 %s:%d", host1, port1)
	host2, port2 := srv2.HostPort()
	log.Infof("server2 %s:%d", host2, port2)

	srv1.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host1, Port: int32(port1)},
				{NodeID: 2, Host: host2, Port: int32(port2)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   1,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
						{
							ID:       1,
							Leader:   1,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
					},
				},
			},
		}
	})
	srv1.Handle(ProduceRequest, func(request Serializable) Serializable {
		req := request.(*proto.ProduceReq)
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						{
							ID:     0,
							Offset: 12345,
						},
					},
				},
			},
		}
	})
	// after closing first server, which started as leader, broker should ask
	// other nodes about the leader and refresh connections
	srv2.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host1, Port: int32(port1)},
				{NodeID: 2, Host: host2, Port: int32(port2)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   2,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
						{
							ID:       1,
							Leader:   2,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
					},
				},
			},
		}
	})
	srv2.Handle(ProduceRequest, func(request Serializable) Serializable {
		req := request.(*proto.ProduceReq)
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						{
							ID:     0,
							Offset: 12346,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-broken-pipe",
		[]string{srv1.Address()},
		s.newTestBrokerConf("test-epipe"))
	c.Assert(err, IsNil)

	data := []byte(strings.Repeat(`
http://stackoverflow.com/questions/11200510/how-to-simulate-abnormal-case-for-socket-tcp-programming-in-linux-such-as-termi

How to get the error EPIPE?
To get the error EPIPE, you need to send large amount of data after closing the socket on the peer side. You can get more info about EPIPE error from this SO Link. I had asked a question about Broken Pipe Error in the link provided and the accepted answer gives a detailed explanation. It is important to note that to get EPIPE error you should have set the flags parameter of send to MSG_NOSIGNAL. Without that, an abnormal send can generate SIGPIPE signal.
	`, 1000))

	pconf := NewProducerConf()
	pconf.RetryWait = time.Millisecond
	pconf.RequestTimeout = time.Millisecond * 20
	producer := broker.Producer(pconf)
	waitForMetadataEpoch(c, producer, 1)
	// produce whatever to fill the cache
	_, err = producer.Produce(
		"test", 0, &proto.Message{Value: data})
	c.Assert(err, IsNil)

	srv1.Close()

	_, err = producer.Produce(
		"test", 0, &proto.Message{Value: data})
	c.Assert(err, Not(IsNil))

	waitForMetadataEpoch(c, producer, 2)
	_, err = producer.Produce(
		"test", 0, &proto.Message{Value: data})
	c.Assert(err, IsNil)
}

func (s *BrokerSuite) TestFetchOffset(c *C) {
	const offset = 94

	srv := NewServer()
	srv.Start()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		off := req.Topics[0].Partitions[0].FetchOffset
		if off != offset {
			panic(fmt.Sprintf("expected fetch offset to be 3, got %d", off))
		}
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID: 0,
							Messages: []*proto.Message{
								{Offset: offset},
							},
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-fetch-offset",
		[]string{srv.Address()},
		s.newTestBrokerConf("test-fetch-offset"))
	c.Assert(err, IsNil)

	conf := NewConsumerConf("test", 0)
	conf.StartOffset = offset
	consumer, err := broker.Consumer(conf)
	c.Assert(err, IsNil)

	msg, err := consumer.Consume()
	c.Assert(err, IsNil)
	c.Assert(msg.Offset, Equals, int64(offset))
}

func (s *BrokerSuite) TestConsumerBrokenPipe(c *C) {
	srv1 := NewServer()
	srv1.Start()
	srv2 := NewServer()
	srv2.Start()

	host1, port1 := srv1.HostPort()
	host2, port2 := srv2.HostPort()

	longBytes := []byte(strings.Repeat(`xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`, 1000))

	srv1.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host1, Port: int32(port1)},
				{NodeID: 2, Host: host2, Port: int32(port2)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   1,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
						{
							ID:       1,
							Leader:   1,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
					},
				},
			},
		}
	})
	srv1.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID: 0,
							Messages: []*proto.Message{
								{Offset: 0, Value: longBytes},
							},
						},
					},
				},
			},
		}
	})

	// after closing first server, which started as leader, broker should ask
	// other nodes about the leader and refresh connections
	srv2.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host1, Port: int32(port1)},
				{NodeID: 2, Host: host2, Port: int32(port2)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       0,
							Leader:   2,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
						{
							ID:       1,
							Leader:   2,
							Replicas: []int32{1, 2},
							Isrs:     []int32{1, 2},
						},
					},
				},
			},
		}
	})
	srv2.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID: 0,
							Messages: []*proto.Message{
								{Offset: 1, Value: longBytes},
							},
						},
					},
				},
			},
		}
	})

	bconf := s.newTestBrokerConf("test-epipe")
	broker, err := NewBroker("test-cluster-consumer-broken-pipe", []string{srv1.Address()}, bconf)
	c.Assert(err, IsNil)

	conf := NewConsumerConf("test", 0)
	conf.RetryErrWait = time.Millisecond
	conf.RetryWait = time.Millisecond
	conf.StartOffset = 0
	consumer, err := broker.Consumer(conf)
	c.Assert(err, IsNil)

	_, err = consumer.Consume()
	c.Assert(err, IsNil)

	srv1.Close()

	// this should succeed after reconnecting to second node
	_, err = consumer.Consume()
	c.Assert(err, IsNil)
}

func (s *BrokerSuite) TestOffsetCoordinator(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	setOffset := int64(-1)

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())
	srv.Handle(GroupCoordinatorRequest, func(request Serializable) Serializable {
		req := request.(*proto.GroupCoordinatorReq)
		host, port := srv.HostPort()
		return &proto.GroupCoordinatorResp{
			CorrelationID:   req.CorrelationID,
			Err:             nil,
			CoordinatorID:   1,
			CoordinatorHost: host,
			CoordinatorPort: int32(port),
		}
	})
	srv.Handle(OffsetCommitRequest, func(request Serializable) Serializable {
		req := request.(*proto.OffsetCommitReq)
		setOffset = req.Topics[0].Partitions[0].Offset
		return &proto.OffsetCommitResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.OffsetCommitRespTopic{
				{
					Name: "first-topic",
					Partitions: []proto.OffsetCommitRespPartition{
						{
							ID:  0,
							Err: nil,
						},
					},
				},
			},
		}
	})
	srv.Handle(OffsetFetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.OffsetFetchReq)
		var partition proto.OffsetFetchRespPartition
		if setOffset == -1 {
			partition.Err = proto.ErrUnknownTopicOrPartition
		} else {
			partition = proto.OffsetFetchRespPartition{
				ID:       0,
				Offset:   int64(setOffset),
				Err:      nil,
				Metadata: "random data",
			}
		}
		return &proto.OffsetFetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.OffsetFetchRespTopic{
				{
					Name:       "first-topic",
					Partitions: []proto.OffsetFetchRespPartition{partition},
				},
			},
		}
	})

	conf := s.newTestBrokerConf("tester")
	broker, err := NewBroker("test-cluster-offset-coordinator", []string{srv.Address()}, conf)
	c.Assert(err, IsNil)

	coordConf := NewOffsetCoordinatorConf("test-group")
	coordinator, err := broker.OffsetCoordinator(coordConf)
	c.Assert(err, IsNil)

	if off, meta, err := coordinator.Offset("does-not-exists", 1423); err == nil {
		c.Fatalf("expected error, got %d, %q", off, meta)
	}
	_, _, err = coordinator.Offset("first-topic", 0)
	c.Assert(err, Equals, proto.ErrUnknownTopicOrPartition)

	err = coordinator.Commit("first-topic", 0, 421)
	c.Assert(err, IsNil)

	off, meta, err := coordinator.Offset("first-topic", 0)
	c.Assert(err, IsNil)
	if off != 421 || meta != "random data" {
		c.Fatalf("unexpected data %d and %q", off, meta)
	}
}

func (s *BrokerSuite) TestOffsetCoordinatorNoCoordinatorError(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())
	srv.Handle(GroupCoordinatorRequest, func(request Serializable) Serializable {
		req := request.(*proto.GroupCoordinatorReq)
		return &proto.GroupCoordinatorResp{
			CorrelationID:   req.CorrelationID,
			Err:             proto.ErrNoCoordinator,
			CoordinatorID:   0,
			CoordinatorHost: "",
			CoordinatorPort: 0,
		}
	})

	conf := s.newTestBrokerConf("tester")
	broker, err := NewBroker("test-cluster-not-coordinator", []string{srv.Address()}, conf)
	c.Assert(err, IsNil)

	coordConf := NewOffsetCoordinatorConf("test-group")
	oc, err := broker.OffsetCoordinator(coordConf)
	c.Assert(err, IsNil)

	err = oc.Commit("foo", int32(0), 10)
	c.Assert(err, Equals, proto.ErrNoCoordinator)
}

func (s *BrokerSuite) BenchmarkConsumer_10Msgs(c *C)    { s.benchmarkConsumer(c, 10) }
func (s *BrokerSuite) BenchmarkConsumer_100Msgs(c *C)   { s.benchmarkConsumer(c, 100) }
func (s *BrokerSuite) BenchmarkConsumer_500Msgs(c *C)   { s.benchmarkConsumer(c, 500) }
func (s *BrokerSuite) BenchmarkConsumer_2000Msgs(c *C)  { s.benchmarkConsumer(c, 2000) }
func (s *BrokerSuite) BenchmarkConsumer_10000Msgs(c *C) { s.benchmarkConsumer(c, 10000) }

// this is not the best benchmark, because Server implementation is
// not made for performance, but it should be good enough to help tuning code.
func (s *BrokerSuite) benchmarkConsumer(c *C, messagesPerResp int) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	var msgOffset int64
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		messages := make([]*proto.Message, messagesPerResp)

		for i := range messages {
			msgOffset++
			msg := &proto.Message{
				Offset: msgOffset,
				Value:  []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur.`),
			}
			messages[i] = msg
		}
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        0,
							TipOffset: msgOffset - int64(len(messages)),
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-benchmark-consumer",
		[]string{srv.Address()},
		s.newTestBrokerConf("test"))
	c.Assert(err, IsNil)

	conf := NewConsumerConf("test", 0)
	conf.StartOffset = 0

	consumer, err := broker.Consumer(conf)
	c.Assert(err, IsNil)

	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		_, err := consumer.Consume()
		c.Assert(err, IsNil)
	}
}

func (s *BrokerSuite) BenchmarkConsumerConcurrent_8Consumers(c *C) {
	s.benchmarkConsumerConcurrent(c, 8)
}
func (s *BrokerSuite) BenchmarkConsumerConcurrent_32Consumers(c *C) {
	s.benchmarkConsumerConcurrent(c, 32)
}
func (s *BrokerSuite) BenchmarkConsumerConcurrent_64Consumers(c *C) {
	s.benchmarkConsumerConcurrent(c, 64)
}

// this is not the best benchmark, because Server implementation is
// not made for performance, but it should be good enough to help tuning code.
func (s *BrokerSuite) benchmarkConsumerConcurrent(c *C, concurrentConsumers int) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	var msgOffset int64
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		messages := make([]*proto.Message, concurrentConsumers*2000)

		for i := range messages {
			msgOffset++
			msg := &proto.Message{
				Offset: msgOffset,
				Value:  []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur.`),
			}
			messages[i] = msg
		}
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        0,
							TipOffset: msgOffset - int64(len(messages)),
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker(
		"test-cluster-benchmark-consumer-concurrent",
		[]string{srv.Address()},
		s.newTestBrokerConf("test"))
	c.Assert(err, IsNil)

	conf := NewConsumerConf("test", 0)
	conf.StartOffset = 0

	consumer, err := broker.Consumer(conf)
	c.Assert(err, IsNil)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(concurrentConsumers)
	for i := 0; i < concurrentConsumers; i++ {
		go func(cn Consumer) {
			defer wg.Done()
			for i := 0; i < c.N/concurrentConsumers; i++ {
				_, err := cn.Consume()
				c.Assert(err, IsNil)
			}
		}(consumer)
	}

	c.ResetTimer()
	close(start)
	wg.Wait()
}

func (s *BrokerSuite) BenchmarkProducer_1Msgs(c *C)    { s.benchmarkProducer(c, 1) }
func (s *BrokerSuite) BenchmarkProducer_2Msgs(c *C)    { s.benchmarkProducer(c, 2) }
func (s *BrokerSuite) BenchmarkProducer_10Msgs(c *C)   { s.benchmarkProducer(c, 10) }
func (s *BrokerSuite) BenchmarkProducer_50Msgs(c *C)   { s.benchmarkProducer(c, 50) }
func (s *BrokerSuite) BenchmarkProducer_200Msgs(c *C)  { s.benchmarkProducer(c, 200) }
func (s *BrokerSuite) BenchmarkProducer_1000Msgs(c *C) { s.benchmarkProducer(c, 1000) }

func (s *BrokerSuite) benchmarkProducer(c *C, messagesPerReq int64) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, NewMetadataHandler(srv, false).Handler())

	var msgOffset int64
	srv.Handle(ProduceRequest, func(request Serializable) Serializable {
		req := request.(*proto.ProduceReq)
		msgOffset += messagesPerReq
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						{
							ID:     0,
							Offset: msgOffset,
						},
					},
				},
			},
		}
	})

	broker, err := NewBroker("test-cluster-benchmark-producer", []string{srv.Address()}, s.newTestBrokerConf("tester"))
	c.Assert(err, IsNil)

	messages := make([]*proto.Message, messagesPerReq)
	for i := range messages {
		msg := &proto.Message{
			Value: []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur.`),
		}
		messages[i] = msg
	}

	producer := broker.Producer(NewProducerConf())

	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		_, err = producer.Produce("test", 0, messages...)
		c.Assert(err, IsNil)
	}
}
