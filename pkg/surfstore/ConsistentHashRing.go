package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap    map[string]string
	ServerAddrs  []string
	ServerHashes []string
	Cache        map[string]string
}

func getServerName(addr string) string {
	return "blockstore" + addr
}

func (c *ConsistentHashRing) init() {
	for _, addr := range c.ServerAddrs {
		hash := c.Hash(getServerName(addr))
		c.ServerMap[hash] = addr
		c.ServerHashes = append(c.ServerHashes, hash)
	}
	sort.Strings(c.ServerHashes)
}

func (c *ConsistentHashRing) GetResponsibleServer(blockId string) string {
	if addr, ok := c.Cache[blockId]; ok {
		return addr
	}
	// Find the first server whose hash is greater than the block hash using binary search
	var addr string
	if blockId > c.ServerHashes[len(c.ServerHashes)-1] {
		addr = c.ServerMap[c.ServerHashes[0]]
	} else {
		i := sort.SearchStrings(c.ServerHashes, blockId)
		addr = c.ServerMap[c.ServerHashes[i]]
	}
	c.Cache[blockId] = addr
	return addr
}

func (c *ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	ring := &ConsistentHashRing{
		ServerMap:    make(map[string]string),
		ServerAddrs:  serverAddrs,
		ServerHashes: []string{},
		Cache:        make(map[string]string),
	}
	ring.init()
	return ring
}
