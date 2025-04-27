package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/valyala/bytebufferpool"
)

type ChunkType = byte
type SegmentID = uint32

// Chunk类型
const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	// ErrClosed 段文件关闭错误
	ErrClosed = errors.New("the segment file is closed")
	// ErrInvalidCRC 无效CRC错误(数据损坏错误)
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	// 7 Bytes
	// Checksum Length Type
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * KB

	fileModePerm = 0644

	// uin32 + uint32 + int64 + uin32
	// segmentId + BlockNumber + ChunkOffset + ChunkSize
	maxLen = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

// Segment represents a single segment file in WAL.
// The segment file is append-only, and the data is written in blocks.
// Each block is 32KB, and the data is written in chunks.
//
// segment: 表示WAL中的单个段文件
// 段文件是追加写入的，写入数据以block块为单位
// 每个block块为 32KB，写入数据以chunk块为单位
type segment struct {
	// 段文件ID
	id SegmentID
	// 文件句柄，用于操作段文件
	fd *os.File
	// 当前block块的块号
	currentBlockNumber uint32
	// 当前block块的大小
	currentBlockSize uint32
	// 关闭标识
	closed bool
	// 段文件首部信息
	header []byte
	// 启动块？
	startupBlock *startupBlock
	// 是否遍历启动？
	isStartupTraversal bool
}

// segmentReader is used to iterate all the data from the segment file.
// You can call Next to get the next chunk data,
// and io.EOF will be returned when there is no data.
//
// segmentReader 用于从段文件中遍历所有数据
// 你可以调用 Next 函数获取下一个chunk数据, 并且当没有数据时将返回io.EOF
type segmentReader struct {
	// 段文件指针
	segment *segment
	// block块的块号
	blockNumber uint32
	// chunk块的写入偏移
	chunkOffset int64
}

// There is only one reader(single goroutine) for startup traversal,
// so we can use one block to finish the whole traversal
// to avoid memory allocation.
//
// 启动遍历只有一个读取器（单个 goroutine），因此我们可以使用一个 block 来完成整个遍历，以避免内存分配
// 如果要顺序迭代整个数据库时，startupBlock可以避免分配内存带来的额外消耗
type startupBlock struct {
	// block块
	block []byte
	// block块的块号
	blockNumber int64
}

// ChunkPosition represents the position of a chunk in a segment file.
// Used to read the data from the segment file.
//
// ChunkPosition: 表示chunk块在段文件中的位置
// 常用于从段文件中读取数据
type ChunkPosition struct {
	// SegmentId 段文件ID
	SegmentId SegmentID
	// BlockNumber The block number of the chunk in the segment file.
	// BlockNumber 段文件中chunk块所在block块的块号
	BlockNumber uint32
	// ChunkOffset The start offset of the chunk in the segment file.
	// ChunkOffset 段文件中的chunk块的写入偏移
	ChunkOffset int64
	// ChunkSize How many bytes the chunk data takes up in the segment file.
	// ChunkSize 段文件中chunk块数据所占的字节数。
	ChunkSize uint32
}

// block缓存池
var blockPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

// 获取block缓存
func getBuffer() []byte {
	return blockPool.Get().([]byte)
}

// 释放block缓存
func putBuffer(buf []byte) {
	blockPool.Put(buf)
}

// todo：实际上这里不应该被称作打开一个新的段文件，它仅仅是打开段文件的作用
// openSegmentFile a new segment file.
// openSegmentFile: 打开一个的段文件
func openSegmentFile(dirPath, extName string, id uint32) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	// set the current block number and block size.
	// 设置当前block块的块号和块大小
	// fd.Seek:
	//		offset：设置下一次读取或写入文件的偏移量
	//		whence：设置偏移原点
	//			io.SeekStart(0) 表示相对于文件的原点进行偏移
	//			io.SeekCurrent(1) 表示相对于当前偏移量进行偏移
	//			io.SeekEnd(2) 表示相对于文件的终点进行偏移
	// 因此，如果打开的是空文件, 则offset=0，否则offset=文件的末尾位置
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err)
	}

	// 返回段文件结构体
	return &segment{
		id:     id,
		fd:     fd,
		header: make([]byte, chunkHeaderSize),
		// 如果打开的是空文件, 则currentBlockNumber=0，否则为该segment文件最后一个没写完的block块的块号
		currentBlockNumber: uint32(offset / blockSize),
		// 如果打开的是空文件, 则currentBlockSize=0，否则为该segment文件最后一个没写完的block块的块大小
		currentBlockSize: uint32(offset % blockSize),
		startupBlock: &startupBlock{
			block:       make([]byte, blockSize),
			blockNumber: -1,
		},
		isStartupTraversal: false,
	}, nil
}

// NewReader creates a new segment reader.
// You can call Next to get the next chunk data,
// and io.EOF will be returned when there is no data.
//
// NewReader 创建一个新的段文件读取器
// 你可以通过调用 Next 函数来获取下一个chunk数据, 并且将会在没有数据时返回io.EOF
func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

// Sync flushes the segment file to disk.
// Sync 将段文件刷写到磁盘上
func (seg *segment) Sync() error {
	// 如果段文件已经关闭，返回nil
	if seg.closed {
		return nil
	}
	// 段文件刷写
	return seg.fd.Sync()
}

// Remove removes the segment file.
// Remove 删除段文件
func (seg *segment) Remove() error {
	// 如果段文件已经关闭，则直接执行os删除操作
	// 否则先将段文件关闭，再执行os删除操作
	if !seg.closed {
		if err := seg.fd.Close(); err != nil {
			return err
		}
		seg.closed = true
	}
	return os.Remove(seg.fd.Name())
}

// Close closes the segment file.
// Close 关闭段文件
func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}
	if err := seg.fd.Close(); err != nil {
		return err
	}
	seg.closed = true
	return nil
}

// Size returns the size of the segment file.
// Size 返回段文件大小
// block的块号是从0开始的，因此块号*块大小为前n-1块的数据大小
// 前n-1块的数据大小 + 当前块的数据大小 = 段文件的数据大小
func (seg *segment) Size() int64 {
	size := int64(seg.currentBlockNumber) * int64(blockSize)
	return size + int64(seg.currentBlockSize)
}

// writeToBuffer calculate chunkPosition for data, write data to bytebufferpool, update segment status
// The data will be written in chunks, and the chunk has four types:
// ChunkTypeFull, ChunkTypeFirst, ChunkTypeMiddle, ChunkTypeLast.
//
// Each chunk has a header, and the header contains the length, type and checksum.
// And the payload of the chunk is the real data you want to Write.
//
// writeToBuffer 计算数据的chunk块位置(chunkPosition), 将数据写入 字节缓冲池(bytebufferpool), 更新段文件(segment)的状态
// 数据将以chunk块为单位写入，chunk块有四种类型: ChunkTypeFull, ChunkTypeFirst, ChunkTypeMiddle, ChunkTypeLast.
// 每一个chunk块都有一个首部，首部包含：长度、类型、chunk块大小
// 而chunk块的有效载荷就是你要写入的真实数据
func (seg *segment) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	// 获取字节缓冲数组的长度
	startBufferLen := chunkBuffer.Len()
	padding := uint32(0)

	// 如果块文件已经关闭，则返回关闭错
	if seg.closed {
		return nil, ErrClosed
	}

	// if the left block size can not hold the chunk header, padding the block
	// 如果左侧block块的大小无法容纳chunk块首部，则对block块进行填充
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		// padding if necessary
		if seg.currentBlockSize < blockSize {
			padding = blockSize - seg.currentBlockSize
			p := make([]byte, padding)
			chunkBuffer.B = append(chunkBuffer.B, p...)

			// a new block
			seg.currentBlockNumber += 1
			seg.currentBlockSize = 0
		}
	}

	// return the start position of the chunk, then the user can use it to read the data.
	// 返回数据块的起始位置，然后用户就可以使用它来读取数据。
	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}

	// 获取待写数据大小
	dataSize := uint32(len(data))
	// The entire chunk can fit into the block.
	// 整个chunk块都可以放入block块中

	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		seg.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		position.ChunkSize = dataSize + chunkHeaderSize
	} else {
		// If the size of the data exceeds the size of the block,
		// the data should be written to the block in batches.
		// 如果数据大小超过block块的大小，则应分批将数据写入block块。
		var (
			leftSize             = dataSize
			blockCount    uint32 = 0
			currBlockSize        = seg.currentBlockSize
		)

		// todo:这里的逻辑值得反复品味
		for leftSize > 0 {
			// 第一次的chunkSize是最新Block剩余可写入的数据大小
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				chunkSize = leftSize
			}

			// 第一次的end = chunkSize
			var end = dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}

			// append the chunks to the buffer
			// 将chunk块加入缓存
			var chunkType ChunkType
			switch leftSize {
			// 第一次 leftSize = dataSize, 所以是ChunkTypeFirst
			case dataSize: // First chunk
				chunkType = ChunkTypeFirst
			// 最后一次leftSize = chunkSize，所以是ChunkTypeLast
			case chunkSize: // Last chunk
				chunkType = ChunkTypeLast
			default: // Middle chunk
				chunkType = ChunkTypeMiddle
			}
			seg.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)

			leftSize -= chunkSize
			blockCount += 1
			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}

	// the buffer length must be equal to chunkSize+padding length
	// buffer的长度必须等于chunkSize+padding
	endBufferLen := chunkBuffer.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		return nil, fmt.Errorf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			position.ChunkSize+padding, endBufferLen-startBufferLen)
	}

	// update segment status
	// 更新段文件状态
	seg.currentBlockSize += position.ChunkSize
	if seg.currentBlockSize >= blockSize {
		seg.currentBlockNumber += seg.currentBlockSize / blockSize
		seg.currentBlockSize = seg.currentBlockSize % blockSize
	}

	return position, nil
}

// writeAll write batch data to the segment file.
// writeAll 将批数据写入段文件
func (seg *segment) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}

	// if any error occurs, restore the segment status
	// 如果出现任何错误，恢复段文件状态
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize

	// init chunk buffer
	// 初始化chunk buffer
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// write all data to the chunk buffer
	// 将所有数据写入chunk buffer(chunk块缓冲)
	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = seg.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return
		}
		positions[i] = pos
	}
	// write the chunk buffer to the segment file
	// 将chunk buffer写入段文件
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return
}

// Write writes the data to the segment file.
// Write 将数据写入段文件
func (seg *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}

	// if any error occurs, restore the segment status
	// 如果出现任何错误，恢复段文件状态
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize

	// init chunk buffer
	// 初始化 chunk buffer
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// write all data to the chunk buffer
	// 将所有数据写入chunk buffer
	pos, err = seg.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return
	}
	// write the chunk buffer to the segment file
	// 将chunk buffer写入段文件
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}

	return
}

// appendChunkBuffer 增加ChunkBuffer
func (seg *segment) appendChunkBuffer(buf *bytebufferpool.ByteBuffer, data []byte, chunkType ChunkType) {
	//构建segment 首部信息

	// chunk块长度
	// Length	2 Bytes	index:4-5
	binary.LittleEndian.PutUint16(seg.header[4:6], uint16(len(data)))
	// chunk块类型
	// Type	1 Byte	index:6
	seg.header[6] = chunkType
	// 校验和：包括chunk块长度、类型、载荷数据
	// Checksum	4 Bytes index:0-3
	sum := crc32.ChecksumIEEE(seg.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)

	// append the header and data to segment chunk buffer
	// 将首部信息和载荷数据添加进chunk buffer
	buf.B = append(buf.B, seg.header...)
	buf.B = append(buf.B, data...)
}

// write the pending chunk buffer to the segment file
// 将待写chunk buffer写入段文件
func (seg *segment) writeChunkBuffer(buf *bytebufferpool.ByteBuffer) error {
	//如果当前的block块大小大于设定的blockSize，返回block块大小超过最大值错误
	if seg.currentBlockSize > blockSize {
		return errors.New("the current block size exceeds the maximum block size")
	}

	// write the data into underlying file
	// 将数据写入底层文件
	if _, err := seg.fd.Write(buf.Bytes()); err != nil {
		return err
	}

	// the cached block can not be reused again after writes.
	// 用于缓存的block块不能在写入后再次使用
	seg.startupBlock.blockNumber = -1
	return nil
}

// Read reads the data from the segment file by the block number and chunk offset.
// Read 使用block块号和chunk块写入偏移从段文件中读取数据
func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	// 内部read函数
	value, _, err := seg.readInternal(blockNumber, chunkOffset)
	return value, err
}

// readInternal 使用block块号和chunk块写入偏移从段文件中读取数据
func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		block     []byte
		segSize   = seg.Size()
		nextChunk = &ChunkPosition{SegmentId: seg.id}
	)

	// 是否启动遍历
	if seg.isStartupTraversal {
		block = seg.startupBlock.block
	} else {
		block = getBuffer()
		if len(block) != blockSize {
			block = make([]byte, blockSize)
		}
		defer putBuffer(block)
	}

	for {
		size := int64(blockSize)
		offset := int64(blockNumber) * blockSize
		// 如果offset+blockSize > segSize，说明最后一个block块的大小不足blockSize
		// 因此size修改为段文件剩余大小：segSize - offset
		if size+offset > segSize {
			size = segSize - offset
		}

		// 如果chunkOffset>=size(实际block块大小)，说明chunk块写入偏移越界
		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		//如果启动遍历
		if seg.isStartupTraversal {
			// There are two cases that we should read block from file:
			// 1. the acquired block is not the cached one
			// 2. new writes appended to the block, and the block
			// is still smaller than 32KB, we must read it again because of the new writes.

			//在两种情况下，我们应该从文件中读取数据块：
			//1. 获取的数据块不是缓存的数据块
			//2. 有新的写入内容添加到该数据块，且该数据块仍然小于 32KB，由于有新的写入内容，我们必须再次读取该数据块。
			if seg.startupBlock.blockNumber != int64(blockNumber) || size != blockSize {
				// read block from segment file at the specified offset.
				// 根据指定的写入偏移从段文件中读取block块
				_, err := seg.fd.ReadAt(block[0:size], offset)
				if err != nil {
					return nil, nil, err
				}
				// remember the block
				// 记录块号
				seg.startupBlock.blockNumber = int64(blockNumber)
			}
		} else {
			// 根据指定的写入偏移从段文件中读取block块
			if _, err := seg.fd.ReadAt(block[0:size], offset); err != nil {
				return nil, nil, err
			}
		}

		// header
		// 获取首部信息
		header := block[chunkOffset : chunkOffset+chunkHeaderSize]

		// length
		// 获取载荷数据长度
		length := binary.LittleEndian.Uint16(header[4:6])

		// copy data
		// 拷贝载荷数据
		start := chunkOffset + chunkHeaderSize
		result = append(result, block[start:start+int64(length)]...)

		// check sum
		// 检查校验和
		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		// 计算校验和 chunkOffset+4：排除savedSum之外的chunk块长度、类型、载荷数据
		checksum := crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		// 如果校验和不相等则返回无效CRC的错误
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		// type
		chunkType := header[6]

		// 为什么能通过ChunkTypeFull和ChunkTypeLast结束循环？
		// 1、ChunkTypeFull标识代表chunk size < block size，已经将chunk块完整读出
		// 2、ChunkTypeLast标识代表目前读取到的是chunk块最后一部分的数据，已经将chunk块完整读出
		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			// 记录下一个chunk块的元数据
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			// If this is the last chunk in the block, and the left block
			// space are paddings, the next chunk should be in the next block.
			// 如果这是block块中最后一块chunk，并且左侧block块空间被填充了，那么下一个chunk块应该在下一个block块
			// 如果新的写入偏移（ChunkOffset）+ chunk块首部大小（chunkHeaderSize）>= block块大小（blockSize）
			// 那么BlockNumber+1，ChunkOffset重置为0
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			// 结束循环
			break
		}

		// 因此，ChunkTypeFirst、ChunkTypeMiddle标识都代表在当前block块中为将chunk块读取完毕，需要继续前往下一个block块中读取
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

// Next returns the Next chunk data.
// You can call it repeatedly until io.EOF is returned.
//
// Next 返回下一个chunk块数据
// 你可以重复调用它，直到返回 io.EOF
func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	// The segment file is closed
	if segReader.segment.closed {
		return nil, nil, ErrClosed
	}

	// this position describes the current chunk info
	// 这个position描述了当前chunk信息
	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}

	// 读取载荷数据，并返回下一个chunk块元数据
	value, nextChunk, err := segReader.segment.readInternal(
		segReader.blockNumber,
		segReader.chunkOffset,
	)
	if err != nil {
		return nil, nil, err
	}

	// Calculate the chunk size.
	// Remember that the chunk size is just an estimated value,
	// not accurate, so don't use it for any important logic.
	// 计算过chunk块大小
	// 请记住，chunk块大小只是一个估计值，并不准确，所以不要在任何重要逻辑中使用它
	// 这个chunk块大小包含padding的大小
	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(segReader.blockNumber*blockSize + uint32(segReader.chunkOffset))

	// update the position
	// 更新position
	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chunkOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}

// Encode encodes the chunk position to a byte slice.
// Return the slice with the actual occupied elements.
// You can decode it by calling wal.DecodeChunkPosition().
//
// Encode 将chunk position编码为字节数组
// 将返回携带真实存在元素的字节数组
// 你可以通过调用 wal.DecodeChunkPosition() 解码
func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

// EncodeFixedSize encodes the chunk position to a byte slice.
// Return a slice of size "maxLen".
// You can decode it by calling wal.DecodeChunkPosition().
//
// EncodeFixedSize 将chunk position编码为字节数组
// 返回一个最大长度的字节数组
// 你可以通过调用 wal.DecodeChunkPosition() 解码
func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

// encode the chunk position to a byte slice.
// encode 将chunk position编码为字节数组
func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)

	var index = 0
	// SegmentId
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	// BlockNumber
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	// ChunkOffset
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	// ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))

	if shrink {
		return buf[:index]
	}
	return buf
}

// DecodeChunkPosition decodes the chunk position from a byte slice.
// You can encode it by calling wal.ChunkPosition.Encode().
//
// DecodeChunkPosition 从字节数组中解码出chunk position
// 你可以通过调用 wal.ChunkPosition.Encode() 进行编码
func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}

	var index = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	return &ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
