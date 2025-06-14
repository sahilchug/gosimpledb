package file

import (
	"bytes"
	"encoding/binary"
	"sync"
)

// Page represents the contents of a disk block in memory.
// A page is treated as an array of BLOCK_SIZE bytes.
// There are methods to get/set values into this array,
// and to read/write the contents of this array to a disk block.
type Page struct {
	mu       sync.RWMutex
	contents []byte
	fileMgr  *FileManager
}

const (
	// BLOCK_SIZE is the number of bytes in a block.
	// This value is set unreasonably low, so that it is easier
	// to create and test databases having a lot of blocks.
	// A more realistic value would be 4K.
	BLOCK_SIZE = 4096

	// INT_SIZE is the size of an integer in bytes.
	// This value is almost certainly 4, but it is
	// a good idea to encode this value as a constant.
	INT_SIZE = 4
)

// NewPage creates a new page.
func NewPage(fileMgr *FileManager) *Page {
	return &Page{
		contents: make([]byte, BLOCK_SIZE),
		fileMgr:  fileMgr,
	}
}

// StrSize returns the maximum size, in bytes, of a string of length n.
// A string is represented as the encoding of its characters,
// preceded by an integer denoting the number of bytes in this encoding.
func StrSize(n int) int {
	// In Go, strings are UTF-8 encoded by default
	// Each character can take up to 4 bytes in UTF-8
	return INT_SIZE + (n * 4)
}

// Read populates the page with the contents of the specified disk block.
func (p *Page) Read(blk *Block) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fileMgr.Read(blk, p.contents)
}

// Write writes the contents of the page to the specified disk block.
func (p *Page) Write(blk *Block) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fileMgr.Write(blk, p.contents)
}

// Append appends the contents of the page to the specified file.
func (p *Page) Append(filename string) (*Block, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fileMgr.Append(filename, p.contents)
}

// GetInt returns the integer value at a specified offset of the page.
// If an integer was not stored at that location,
// the behavior of the method is unpredictable.
func (p *Page) GetInt(offset int) int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Read 4 bytes starting at offset
	buf := bytes.NewReader(p.contents[offset : offset+INT_SIZE])
	var val int32
	binary.Read(buf, binary.LittleEndian, &val)
	return val
}

// SetInt writes an integer to the specified offset on the page.
func (p *Page) SetInt(offset int, val int32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Write 4 bytes starting at offset
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, val)
	copy(p.contents[offset:], buf.Bytes())
}

// GetString returns the string value at the specified offset of the page.
// If a string was not stored at that location,
// the behavior of the method is unpredictable.
func (p *Page) GetString(offset int) string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// First read the length of the string
	len := p.GetInt(offset)

	// Then read the string bytes
	return string(p.contents[offset+INT_SIZE : offset+INT_SIZE+int(len)])
}

// SetString writes a string to the specified offset on the page.
// We Encode the string as length + string bytes.
func (p *Page) SetString(offset int, val string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// First write the length of the string
	p.SetInt(offset, int32(len(val)))

	// Then write the string bytes
	copy(p.contents[offset+INT_SIZE:], []byte(val))
}
