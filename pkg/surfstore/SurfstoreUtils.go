package surfstore

import (
	"fmt"
	"log"
	"os"
)

const INDEX_NAME = "index.db"
const DELETED_HASH = "0"
const EMPTY_HASH = "-1"

type Logic struct {
	BaseFileMetaMap   map[string]*FileMetaData
	LocalFileMetaMap  map[string]*FileMetaData
	RemoteFileMetaMap map[string]*FileMetaData
	RPCClient         RPCClient
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	logic := Logic{}
	logic.RPCClient = client
	err := logic.ExcecuteLogic()
	if err != nil {
		fmt.Println("Error executing logic:", err)
		log.Fatal(err)
	}
}

func getBlockHashList(fileData []byte, blockSize int) []string {
	blockHashList := []string{}
	if len(fileData) == 0 {
		blockHashList = getEmptyHashList()
	} else {
		for i := 0; i < len(fileData); i += blockSize {
			end := i + blockSize
			if end > len(fileData) {
				end = len(fileData)
			}
			blockData := fileData[i:end]
			blockHash := GetBlockHashString(blockData)
			blockHashList = append(blockHashList, blockHash)
		}
	}
	return blockHashList
}

func areEqualBlockHashLists(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func isDeleted(hashList []string) bool {
	if len(hashList) == 1 && hashList[0] == DELETED_HASH {
		return true
	}
	return false
}

func isEmpty(hashList []string) bool {
	if len(hashList) == 1 && hashList[0] == EMPTY_HASH {
		return true
	}
	return false
}

func getDeletedHashList() []string {
	return []string{DELETED_HASH}
}

func getEmptyHashList() []string {
	return []string{EMPTY_HASH}
}

func copyFileInfo(a *FileMetaData) *FileMetaData {
	b := &FileMetaData{}
	b.Filename = a.Filename
	b.Version = a.Version
	b.BlockHashList = make([]string, len(a.BlockHashList))
	copy(b.BlockHashList, a.BlockHashList)
	return b
}

func invertBlockStoreMap(blockStoreMap map[string][]string) map[string]string {
	invertedBlockStoreMap := make(map[string]string)
	for addr, hashList := range blockStoreMap {
		for _, hash := range hashList {
			invertedBlockStoreMap[hash] = addr
		}
	}
	return invertedBlockStoreMap
}

func (logic *Logic) ExcecuteLogic() error {
	// Load index.db
	err := logic.LoadLocal()
	if err != nil {
		return err
	}
	// Load remote metadata
	err = logic.LoadRomote()
	if err != nil {
		return err
	}
	// Download remote files
	err = logic.SyncRemoteToLocal()
	if err != nil {
		return err
	}
	// Load base directory
	err = logic.LoadBase()
	if err != nil {
		return err
	}
	// Upload updated files
	err = logic.SyncBaseToLocal()
	if err != nil {
		return err
	}
	// Save index.db
	err = logic.SaveLocal()
	if err != nil {
		return err
	}
	return nil
}

func (logic *Logic) LoadLocal() error {
	var err error
	logic.LocalFileMetaMap, err = LoadMetaFromMetaFile(logic.RPCClient.BaseDir)
	if err != nil {
		return err
	}
	return nil
}

func (logic *Logic) LoadRomote() error {
	err := logic.RPCClient.GetFileInfoMap(&logic.RemoteFileMetaMap)
	if err != nil {
		return err
	}
	return nil
}

func (logic *Logic) SyncRemoteToLocal() error {
	for filename, remoteFileInfo := range logic.RemoteFileMetaMap {
		localFileInfo, localExist := logic.LocalFileMetaMap[filename]
		if !localExist || remoteFileInfo.Version > localFileInfo.Version {
			err := logic.DownloadFile(filename)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (logic *Logic) DownloadFile(filename string) error {
	remoteBlockHashList := logic.RemoteFileMetaMap[filename].BlockHashList
	if isDeleted(remoteBlockHashList) {
		// Delete file
		if _, err := os.Stat(ConcatPath(logic.RPCClient.BaseDir, filename)); err == nil {
			err := os.Remove(ConcatPath(logic.RPCClient.BaseDir, filename))
			if err != nil {
				return err
			}
		}
	} else if isEmpty(remoteBlockHashList) {
		// Create empty file
		if _, err := os.Stat(ConcatPath(logic.RPCClient.BaseDir, filename)); err == nil {
			err := os.Remove(ConcatPath(logic.RPCClient.BaseDir, filename))
			if err != nil {
				return err
			}
		}
		err := os.WriteFile(ConcatPath(logic.RPCClient.BaseDir, filename), []byte{}, 0666)
		if err != nil {
			return err
		}
	} else {
		// Download file
		blockStoreMap := make(map[string][]string)
		// Get responsible server for each block
		logic.RPCClient.GetBlockStoreMap(remoteBlockHashList, &blockStoreMap)
		hashAddrMap := invertBlockStoreMap(blockStoreMap)

		fileData := []byte{}
		for _, blockHash := range remoteBlockHashList {
			// Get each block from its responsible server
			var block Block
			err := logic.RPCClient.GetBlock(blockHash, hashAddrMap[blockHash], &block)
			if err != nil {
				return err
			}
			fileData = append(fileData, block.BlockData...)
		}
		err := os.WriteFile(ConcatPath(logic.RPCClient.BaseDir, filename), fileData, 0666)
		if err != nil {
			return err
		}
	}
	logic.LocalFileMetaMap[filename] = copyFileInfo(logic.RemoteFileMetaMap[filename])
	return nil
}

func (logic *Logic) LoadBase() error {
	err := logic.ScanBaseDir()
	if err != nil {
		return err
	}
	logic.CompleteBaseWithLocal()
	return nil
}

func (logic *Logic) ScanBaseDir() error {
	logic.BaseFileMetaMap = make(map[string]*FileMetaData)
	files, err := os.ReadDir(logic.RPCClient.BaseDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		// Calculate the hash list for each file
		if !file.IsDir() && file.Name() != INDEX_NAME {
			fileData, err := os.ReadFile(ConcatPath(logic.RPCClient.BaseDir, file.Name()))
			if err != nil {
				return err
			}
			blockHashList := getBlockHashList(fileData, logic.RPCClient.BlockSize)
			logic.BaseFileMetaMap[file.Name()] = &FileMetaData{
				Filename:      file.Name(),
				Version:       1,
				BlockHashList: blockHashList,
			}
		}
	}
	return nil
}

func (logic *Logic) CompleteBaseWithLocal() {
	for filename, localFileInfo := range logic.LocalFileMetaMap {
		baseFileInfo, baseExist := logic.BaseFileMetaMap[filename]
		// Check if the file is updated or deleted
		if isDeleted(localFileInfo.BlockHashList) {
			if !baseExist {
				logic.BaseFileMetaMap[filename] = copyFileInfo(localFileInfo)
			} else {
				baseFileInfo.Version = localFileInfo.Version + 1
			}
		} else {
			if !baseExist {
				logic.BaseFileMetaMap[filename] = &FileMetaData{
					Filename:      filename,
					Version:       localFileInfo.Version + 1,
					BlockHashList: getDeletedHashList(),
				}
			} else {
				if areEqualBlockHashLists(localFileInfo.BlockHashList, baseFileInfo.BlockHashList) {
					baseFileInfo.Version = localFileInfo.Version
				} else {
					baseFileInfo.Version = localFileInfo.Version + 1
				}
			}
		}
	}
}

func (logic *Logic) SyncBaseToLocal() error {
	for filename, baseFileInfo := range logic.BaseFileMetaMap {
		localFileInfo, localExist := logic.LocalFileMetaMap[filename]
		if !localExist || baseFileInfo.Version > localFileInfo.Version {
			// Upload an updated file
			err := logic.UploadFile(filename)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (logic *Logic) UploadFile(filename string) error {
	baseFileInfo := logic.BaseFileMetaMap[filename]
	baseBlockHashList := baseFileInfo.BlockHashList
	if isDeleted(baseBlockHashList) || isEmpty(baseBlockHashList) {
		// Update remote metadata
		var latestVersion int32
		logic.RPCClient.UpdateFile(copyFileInfo(baseFileInfo), &latestVersion)
		if latestVersion == -1 {
			// Overwrite local file if there is an immediate conflict
			err := logic.ResolveConflict(filename)
			if err != nil {
				return err
			}
		} else {
			logic.LocalFileMetaMap[filename] = copyFileInfo(baseFileInfo)
		}
	} else {
		blockStoreMap := make(map[string][]string)
		logic.RPCClient.GetBlockStoreMap(baseBlockHashList, &blockStoreMap)
		hashAddrMap := invertBlockStoreMap(blockStoreMap)

		// Get missing blocks
		missingBlockHashSet := make(map[string]struct{})
		for addr, blockHashList := range blockStoreMap {
			missingBlockHashList := []string{}
			err := logic.RPCClient.MissingBlocks(blockHashList, addr, &missingBlockHashList)
			if err != nil {
				return err
			}
			for _, blockHash := range missingBlockHashList {
				missingBlockHashSet[blockHash] = struct{}{}
			}
		}

		fileData, err := os.ReadFile(ConcatPath(logic.RPCClient.BaseDir, filename))
		if err != nil {
			return err
		}
		for i := range baseBlockHashList {
			blockHash := baseBlockHashList[i]
			// Only upload missing blocks
			if _, missing := missingBlockHashSet[blockHash]; missing {
				end := (i + 1) * logic.RPCClient.BlockSize
				if end > len(fileData) {
					end = len(fileData)
				}
				blockData := fileData[i*logic.RPCClient.BlockSize : end]
				var block Block
				block.BlockData = blockData
				block.BlockSize = int32(len(blockData))
				var succ bool
				err := logic.RPCClient.PutBlock(&block, hashAddrMap[blockHash], &succ)
				if err != nil {
					return err
				}
				if !succ {
					return fmt.Errorf("put block failed")
				}
			}
		}

		// Update remote metadata
		var latestVersion int32
		logic.RPCClient.UpdateFile(copyFileInfo(baseFileInfo), &latestVersion)
		if latestVersion == -1 {
			// Overwrite local file if there is an immediate conflict
			err := logic.ResolveConflict(filename)
			if err != nil {
				return err
			}
		} else {
			logic.LocalFileMetaMap[filename] = copyFileInfo(baseFileInfo)
		}
	}
	return nil
}

func (logic *Logic) ResolveConflict(filename string) error {
	err := logic.LoadRomote()
	if err != nil {
		return err
	}
	err = logic.DownloadFile(filename)
	if err != nil {
		return err
	}
	return nil
}

func (logic *Logic) SaveLocal() error {
	err := WriteMetaFile(logic.LocalFileMetaMap, logic.RPCClient.BaseDir)
	if err != nil {
		return err
	}
	return nil
}
