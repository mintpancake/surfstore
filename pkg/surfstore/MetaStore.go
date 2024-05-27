package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	newVersion := fileMetaData.Version
	oldVersion := int32(0)
	if _, ok := m.FileMetaMap[filename]; ok {
		oldVersion = m.FileMetaMap[filename].Version
	}
	if newVersion == oldVersion+1 {
		m.FileMetaMap[filename] = fileMetaData
		return &Version{Version: newVersion}, nil
	} else {
		return &Version{Version: -1}, nil
	}
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := &BlockStoreMap{BlockStoreMap: map[string]*BlockHashes{}}
	for _, hash := range blockHashesIn.Hashes {
		addr := m.ConsistentHashRing.GetResponsibleServer(hash)
		if _, ok := blockStoreMap.BlockStoreMap[addr]; !ok {
			blockStoreMap.BlockStoreMap[addr] = &BlockHashes{Hashes: []string{}}
		}
		blockStoreMap.BlockStoreMap[addr].Hashes = append(blockStoreMap.BlockStoreMap[addr].Hashes, hash)
	}
	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
