// +build integration

package kafka

import (
	. "gopkg.in/check.v1"
)

var _ = Suite(&MetadataCacheSuite{})

// This test suite is currently gated behind the 'integration' tag due to a
// dependency on local Kafka.
type MetadataCacheSuite struct{}

func (s *MetadataCacheSuite) SetUpTest(c *C) {
	ResetTestLogger(c)
}

func (s *MetadataCacheSuite) TestMetadataConsistency(c *C) {
	var (
		cache       = newMetadataCache()
		clusterName = "test_cluster"
		addresses   = []string{"localhost:9092"}
		conf        = NewClusterConnectionConf()
	)

	metadataFromCache, err := cache.getOrCreateMetadata(clusterName, addresses, conf)
	c.Assert(err, IsNil)

	sameMetadataFromCache, err := cache.getOrCreateMetadata(clusterName, addresses, conf)
	c.Assert(err, IsNil)

	c.Assert(metadataFromCache, Equals, sameMetadataFromCache)
}
