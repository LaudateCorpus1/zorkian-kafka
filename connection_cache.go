package kafka

import (
	"sync"
)

// connectionPoolCache caches connections to a single cluster by ClientID.
type connectionPoolCache struct {
	lock              sync.Mutex
	connectionPoolMap map[string]*connectionPool
}

// connectionPoolCache is a threadsafe cache of ConnectionPool by clientID.  One connectionPoolCache
// should have ConnectionPools to only one cluster.  The implementation is similar to MetadataCache,
// except that it also keeps track of a list of the ConnectionPools cached so it can refresh the
// addresses on each of them in reinitializeAddrs.
func newConnPoolCache() *connectionPoolCache {
	return &connectionPoolCache{
		lock:              sync.Mutex{},
		connectionPoolMap: make(map[string]*connectionPool),
	}

}

// getOrCreateConnectionPool creates or gets the existing broker from the connectionPoolCache for
// the given (serviceName, clientId) tuple.
func (c *connectionPoolCache) getOrCreateConnectionPool(
	clientID string, conf ClusterConnectionConf, nodeAddresses []string) (
	*connectionPool, error) {

	c.lock.Lock()
	defer c.lock.Unlock()

	log.Infof("Retrieving connection pool for clientID %s from LockingMap", clientID)
	if connectionPool, ok := c.connectionPoolMap[clientID]; ok {
		return connectionPool, nil
	}
	log.Infof("ConnectionPool for cluster %s being created.", nodeAddresses)

	connPool := newConnectionPool(conf, nodeAddresses)
	c.connectionPoolMap[clientID] = connPool
	return connPool, nil
}

func (c *connectionPoolCache) reinitializeAddrs(nodeAddresses []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, pool := range c.connectionPoolMap {
		pool.InitializeAddrs(nodeAddresses)
	}
}
