package kafka

import (
	"errors"
	"sync"
	"time"

	. "gopkg.in/check.v1"

	"github.com/discord/zorkian-kafka/proto"
)

var _ = Suite(&DistProducerSuite{})

type DistProducerSuite struct{}

func (s *DistProducerSuite) SetUpTest(c *C) {
	ResetTestLogger(c)
}

var ErrTestPartitionDisabled = errors.New("partition disabled by test")

var testMessageData = [][][]byte{
	{
		[]byte("a 1"),
		[]byte("a 2"),
	},
	{
		[]byte("b 1"),
	},
	{
		[]byte("c 1"),
		[]byte("c 2"),
		[]byte("c 3"),
	},
	{
		[]byte("d 1"),
	},
	{
		[]byte("e 1"),
		[]byte("e 2"),
	},
	{
		[]byte("f 1"),
	},
}

type recordingProducer struct {
	sync.Mutex
	msgs               []*proto.Message
	disabledPartitions map[int32]struct{}
	disabledWrites     int
}

func newRecordingProducer(disabledPartitions map[int32]struct{}) *recordingProducer {
	return &recordingProducer{
		msgs:               make([]*proto.Message, 0),
		disabledPartitions: disabledPartitions,
	}
}

func (p *recordingProducer) Produce(topic string, part int32, msgs ...*proto.Message) (int64, error) {
	p.Lock()
	defer p.Unlock()

	// This is sort of horrible, but we are in a race with partitionData.reEnqueue
	time.Sleep(100 * time.Millisecond)

	if _, ok := p.disabledPartitions[part]; ok {
		p.disabledWrites++
		return 0, ErrTestPartitionDisabled
	}

	offset := len(p.msgs)
	for i, msg := range msgs {
		msg.Offset = int64(offset + i)
		msg.Topic = topic
		msg.Partition = part
	}
	p.msgs = append(p.msgs, msgs...)

	return int64(len(p.msgs)), nil
}

type dummyPartitionCountSource struct {
	impl func(string) (int32, error)
}

func (p *dummyPartitionCountSource) PartitionCount(topic string) (int32, error) {
	return p.impl(topic)
}

/* This test is currently disabled. Partition ordering is randomized, so result
// order can't be anticipated.
func (s *DistProducerSuite) TestErrorAverseRRProducerBasics(c *C) {
	rec := newRecordingProducer(nil)
	conf := NewErrorAverseRRProducerConf()
	conf.PartitionCountSource = &dummyPartitionCountSource{
		impl: func(string) (int32, error) { return 3, nil },
	}
	conf.Producer = rec
	conf.PartitionFetchTimeout = time.Second
	p := NewErrorAverseRRProducer(conf)

	for i, values := range testMessageData {
		msgs := make([]*proto.Message, 0)
		for _, value := range values {
			msgs = append(msgs, &proto.Message{Value: value})
		}
		if _, _, err := p.Distribute("test-topic", msgs...); err != nil {
			c.Errorf("cannot distribute %d message: %s", i, err)
		}
	}

	expected := map[int]int32{
		0: 0,
		1: 0,
		2: 1,
		3: 2,
		4: 2,
		5: 2,
		6: 0,
		7: 1,
		8: 1,
		9: 2,
	}

	c.Assert(len(rec.msgs), Equals, len(expected))
	for i, partition := range expected {
		if rec.msgs[i].Partition != partition {
			c.Errorf("Wrong partition number for message %d. Expected %d but got %d.", i, partition, rec.msgs[i].Partition)
		}
	}
	c.Assert(rec.disabledWrites, Equals, 0)
}
*/

/* This test is currently disabled. Partition ordering is randomized, so result
// order can't be anticipated.
func (s *DistProducerSuite) TestErrorAverseRRProducerDeadPartition(c *C) {
	rec := newRecordingProducer(map[int32]struct{}{
		1: struct{}{},
	})
	conf := NewErrorAverseRRProducerConf()
	conf.PartitionCountSource = &dummyPartitionCountSource{
		impl: func(string) (int32, error) { return 3, nil },
	}
	conf.Producer = rec
	conf.PartitionFetchTimeout = time.Second
	p := NewErrorAverseRRProducer(conf)

	for i, values := range testMessageData {
		msgs := make([]*proto.Message, 0)
		for _, value := range values {
			msgs = append(msgs, &proto.Message{Value: value})
		}
		if i == 1 || i == 3 {
			if _, _, err := p.Distribute("test-topic", msgs...); err == nil {
				c.Errorf("Should have failed to write message %d: %s", i, err)
			}
		}
		if _, _, err := p.Distribute("test-topic", msgs...); err != nil {
			c.Errorf("cannot distribute %d message: %s", i, err)
		}
	}

	expected := map[int]int32{
		0: 0,
		1: 0,
		2: 2,
		3: 0,
		4: 0,
		5: 0,
		6: 2,
		7: 0,
		8: 0,
		9: 2,
	}

	c.Assert(len(rec.msgs), Equals, len(expected))
	for i, partition := range expected {
		if rec.msgs[i].Partition != partition {
			c.Errorf("Wrong partition number for message %d. Expected %d but got %d.", i, partition, rec.msgs[i].Partition)
		}
	}
	c.Assert(rec.disabledWrites, Equals, 2)
}
*/

/* This test is currently disabled. Partition ordering is randomized, so result
// order can't be anticipated.
func (s *DistProducerSuite) TestErrorAverseRRProducerDeadPartitions(c *C) {
	rec := newRecordingProducer(map[int32]struct{}{
		0: struct{}{},
		2: struct{}{},
	})
	conf := NewErrorAverseRRProducerConf()
	conf.PartitionCountSource = &dummyPartitionCountSource{
		impl: func(string) (int32, error) { return 3, nil },
	}
	conf.Producer = rec
	conf.PartitionFetchTimeout = time.Second
	p := NewErrorAverseRRProducer(conf)

	for i, values := range testMessageData {
		msgs := make([]*proto.Message, 0)
		for _, value := range values {
			msgs = append(msgs, &proto.Message{Value: value})
		}
		if i == 0 || i == 2 {
			if _, _, err := p.Distribute("test-topic", msgs...); err == nil {
				c.Errorf("Should have failed to write message %d: %s", i, err)
			}
			if _, _, err := p.Distribute("test-topic", msgs...); err != nil {
				c.Errorf("cannot distribute %d message: %s", i, err)
			}
		} else if i == 1 {
			if _, _, err := p.Distribute("test-topic", msgs...); err == nil {
				c.Errorf("Should have failed to write message %d: %s", i, err)
			}
			if _, _, err := p.Distribute("test-topic", msgs...); err == nil {
				c.Errorf("Should have failed to write message %d: %s", i, err)
			}
			if _, _, err := p.Distribute("test-topic", msgs...); err != nil {
				c.Errorf("cannot distribute %d message: %s", i, err)
			}
		} else {
			if _, _, err := p.Distribute("test-topic", msgs...); err != nil {
				c.Errorf("cannot distribute %d message: %s", i, err)
			}
		}
	}

	expected := map[int]int32{
		0: 1,
		1: 1,
		2: 1,
		3: 1,
		4: 1,
		5: 1,
		6: 1,
		7: 1,
		8: 1,
		9: 1,
	}

	c.Assert(len(rec.msgs), Equals, len(expected))
	for i, partition := range expected {
		if rec.msgs[i].Partition != partition {
			c.Errorf("Wrong partition number for message %d. Expected %d but got %d.", i, partition, rec.msgs[i].Partition)
		}
	}
	c.Assert(rec.disabledWrites, Equals, 4)
}
*/

func (s *DistProducerSuite) TestErrorAverseRRProducerAllDeadPartitions(c *C) {
	rec := newRecordingProducer(map[int32]struct{}{
		0: struct{}{},
		1: struct{}{},
		2: struct{}{},
	})
	conf := NewErrorAverseRRProducerConf()
	conf.PartitionCountSource = &dummyPartitionCountSource{
		impl: func(string) (int32, error) { return 3, nil },
	}
	conf.Producer = rec
	conf.PartitionFetchTimeout = time.Second
	p := NewErrorAverseRRProducer(conf)

	for _, values := range testMessageData {
		msgs := make([]*proto.Message, 0)
		for _, value := range values {
			msgs = append(msgs, &proto.Message{Value: value})
		}
		_, _, err := p.Distribute("test-topic", msgs...)
		c.Assert(err, Equals, ErrTestPartitionDisabled)
	}

	if len(rec.msgs) > 0 {
		c.Errorf("Should have failed to write all messages, but saw %v", rec.msgs)
	}
	c.Assert(rec.disabledWrites, Equals, 6)
}

/* This test is currently disabled. Partition ordering is randomized, so result
// order can't be anticipated.
func (s *DistProducerSuite) TestErrorAverseRRProducerIncreasePartitionCount(c *C) {
	rec := newRecordingProducer(nil)
	conf := NewErrorAverseRRProducerConf()
	var numPartitions int32 = 3
	conf.PartitionCountSource = &dummyPartitionCountSource{
		impl: func(string) (int32, error) { return numPartitions, nil },
	}
	conf.Producer = rec
	conf.PartitionFetchTimeout = time.Second
	p := NewErrorAverseRRProducer(conf)

	for i, values := range testMessageData {
		msgs := make([]*proto.Message, 0)
		for _, value := range values {
			msgs = append(msgs, &proto.Message{Value: value})
		}
		if _, _, err := p.Distribute("test-topic", msgs...); err != nil {
			c.Errorf("cannot distribute %d message: %s", i, err)
		}
	}

	numPartitions = 5

	for i, values := range testMessageData {
		msgs := make([]*proto.Message, 0)
		for _, value := range values {
			msgs = append(msgs, &proto.Message{Value: value})
		}
		if _, _, err := p.Distribute("test-topic", msgs...); err != nil {
			c.Errorf("cannot distribute %d message: %s", i, err)
		}
	}

	expected := map[int]int32{
		0:  0,
		1:  0,
		2:  1,
		3:  2,
		4:  2,
		5:  2,
		6:  0,
		7:  1,
		8:  1,
		9:  2,
		10: 0,
		11: 0,
		12: 1,
		13: 2,
		14: 2,
		15: 2,
		16: 3,
		17: 4,
		18: 4,
		19: 0,
	}

	c.Assert(len(rec.msgs), Equals, len(expected))
	for i, partition := range expected {
		if rec.msgs[i].Partition != partition {
			c.Errorf("Wrong partition number for message %d. Expected %d but got %d.", i, partition, rec.msgs[i].Partition)
		}
	}
	c.Assert(rec.disabledWrites, Equals, 0)
}
*/
