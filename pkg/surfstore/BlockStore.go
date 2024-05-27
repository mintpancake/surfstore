package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return &Block{}, nil
	} else {
		return block, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	blockHash := GetBlockHashString(block.BlockData)
	bs.BlockMap[blockHash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are NOT stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	missingHashes := []string{}
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; !ok {
			missingHashes = append(missingHashes, hash)
		}
	}
	return &BlockHashes{Hashes: missingHashes}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	allHashes := []string{}
	for hash := range bs.BlockMap {
		allHashes = append(allHashes, hash)
	}
	return &BlockHashes{Hashes: allHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
