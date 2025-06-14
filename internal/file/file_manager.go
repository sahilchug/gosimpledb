package file

import (
	"os"
	"path/filepath"
	"sync"
)

// FileManager manages the reading and writing of blocks to files.
type FileManager struct {
	mu        sync.RWMutex
	dbDir     string
	openFiles map[string]*os.File
}

// NewFileManager creates a new file manager for the specified database directory.
func NewFileManager(dbDir string) (*FileManager, error) {
	// Create the database directory if it doesn't exist
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, err
	}

	return &FileManager{
		dbDir:     dbDir,
		openFiles: make(map[string]*os.File),
	}, nil
}

// Read reads the contents of a block into a byte slice.
func (fm *FileManager) Read(blk *Block, p []byte) error {
	fm.mu.RLock()
	file, exists := fm.openFiles[blk.FileName()]
	fm.mu.RUnlock()

	if !exists {
		var err error
		file, err = os.OpenFile(filepath.Join(fm.dbDir, blk.FileName()), os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		fm.mu.Lock()
		fm.openFiles[blk.FileName()] = file
		fm.mu.Unlock()
	}

	// Seek to the correct position in the file
	_, err := file.Seek(int64(blk.Number()*BLOCK_SIZE), 0)
	if err != nil {
		return err
	}

	// Read the block
	_, err = file.Read(p)
	return err
}

// Write writes the contents of a byte slice to a block.
func (fm *FileManager) Write(blk *Block, p []byte) error {
	fm.mu.RLock()
	file, exists := fm.openFiles[blk.FileName()]
	fm.mu.RUnlock()

	if !exists {
		var err error
		file, err = os.OpenFile(filepath.Join(fm.dbDir, blk.FileName()), os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		fm.mu.Lock()
		fm.openFiles[blk.FileName()] = file
		fm.mu.Unlock()
	}

	// Seek to the correct position in the file
	_, err := file.Seek(int64(blk.Number()*BLOCK_SIZE), 0)
	if err != nil {
		return err
	}

	// Write the block
	_, err = file.Write(p)
	return err
}

// Append appends the contents of a byte slice to a file and returns the block reference.
func (fm *FileManager) Append(filename string, p []byte) (*Block, error) {
	fm.mu.RLock()
	file, exists := fm.openFiles[filename]
	fm.mu.RUnlock()

	if !exists {
		var err error
		file, err = os.OpenFile(filepath.Join(fm.dbDir, filename), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		fm.mu.Lock()
		fm.openFiles[filename] = file
		fm.mu.Unlock()
	}

	// Get the file size to determine the block number
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Calculate the block number
	blknum := int(fileInfo.Size()) / BLOCK_SIZE

	// Seek to the end of the file
	_, err = file.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	// Write the block
	_, err = file.Write(p)
	if err != nil {
		return nil, err
	}

	return NewBlock(filename, blknum), nil
}
