package file

import "fmt"

// Block represents a reference to a disk block.
// A Block object consists of a filename and a block number.
// It does not hold the contents of the block;
// instead, that is the job of a Page object.
type Block struct {
	filename string
	blknum   int
}

// NewBlock constructs a block reference for the specified filename and block number.
func NewBlock(filename string, blknum int) *Block {
	return &Block{
		filename: filename,
		blknum:   blknum,
	}
}

// FileName returns the name of the file where the block lives.
func (b *Block) FileName() string {
	return b.filename
}

// Number returns the location of the block within the file.
func (b *Block) Number() int {
	return b.blknum
}

// Equal checks if two blocks are equal by comparing their filename and block number.
func (b *Block) Equal(other *Block) bool {
	return b.filename == other.filename && b.blknum == other.blknum
}

// String returns a string representation of the block.
func (b *Block) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blknum)
}
