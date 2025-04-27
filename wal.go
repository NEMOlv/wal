package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// 初始化段文件ID
	initialSegmentFileID = 1
)

var (
	// 载荷数据大小不能大于段文件大小
	ErrValueTooLarge = errors.New("the data size can't larger than segment size")
	// 待写数组大小不能大于段文件大小
	ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
)

// WAL represents a Write-Ahead Log structure that provides durability
// and fault-tolerance for incoming writes.
// It consists of an activeSegment, which is the current segment file
// used for new incoming writes, and olderSegments,
// which is a map of segment files used for read operations.
//
// The options field stores various configuration options for the WAL.
//
// The mu sync.RWMutex is used for concurrent access to the WAL data structure,
// ensuring safe access and modification.
//
// WAL是预写式日志结构，为将要写入的数据提供了持久性和容错性。
// 它由 activeSegment 和 olderSegments 两部分组成，前者是当前用于新写入的段文件，后者是用于读取操作的段文件映射。
// options字段存储了各种各样WAL的配置选项。
// mu sync.RWMutex 用于并发访问 WAL 数据结构，确保访问和修改的安全。
type WAL struct {
	// active segment file, used for new incoming writes.
	// 活跃段文件，用于新写入
	activeSegment *segment
	// older segment files, only used for read.
	// 历史段文件，只用于读操作
	olderSegments map[SegmentID]*segment
	// options字段存储了各种各样WAL的配置选项
	options Options
	// 读写锁，用于并发访问 WAL 数据结构，确保访问和修改的安全。
	mu sync.RWMutex
	// 字节写入 ？
	bytesWrite uint32
	// 重命名段ID
	renameIds []SegmentID
	// 待写数组
	pendingWrites [][]byte
	// 待写数组大小
	pendingSize int64
	// 写锁，用于待写数组的写入操作
	pendingWritesLock sync.Mutex
	// 关闭channel
	closeC chan struct{}
	// 同步计时器
	syncTicker *time.Ticker
}

// Reader represents a reader for the WAL.
// It consists of segmentReaders, which is a slice of segmentReader
// structures sorted by segment id,
// and currentReader, which is the index of the current segmentReader in the slice.
//
// The currentReader field is used to iterate over the segmentReaders slice.
//
// Reader 是 WAL 的读取器
// 它由segmentReaders和currentReader两部分组成，前者是由段ID排序的segmentReaders数组，后者是当前segmentReader在segmentReaders数组中的下标
// currentReader 字段用于遍历 segmentReaders数组
type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

// Open opens a WAL with the given options.
// It will create the directory if not exists, and open all segment files in the directory.
// If there is no segment file in the directory, it will create a new one.
//
// Open 用给予的配置选项打开WAL实例
// 如果目录不存在，它将会创建一个，然后它会打开目录中的所有段文件
// 如果目录中没有段文件，它将会创建一个新的段文件
func Open(options Options) (*WAL, error) {
	// 检查段文件扩展名是否有'.'
	if !strings.HasPrefix(options.SegmentFileExt, ".") {
		return nil, fmt.Errorf("segment file extension must start with '.'")
	}

	// 初始化WAL实例
	wal := &WAL{
		options:       options,
		olderSegments: make(map[SegmentID]*segment),
		pendingWrites: make([][]byte, 0),
		closeC:        make(chan struct{}),
	}

	// create the directory if not exists.
	// 如果目录不存在，则新建一个
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	// iterate the dir and open all segment files.
	// 遍历目录并打开所有段文件
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	// get all segment file ids.
	// 获取所有段文件ID
	var segmentIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		//格式化匹配：从段文件名中取出ID值赋予id变量
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileExt, &id)
		if err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, id)
	}

	// empty directory, just initialize a new segment file.
	// 如果是空目录，则初始化一个新的段文件
	if len(segmentIDs) == 0 {
		// 打开一个新的段文件
		segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt, initialSegmentFileID)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		// open the segment files in order, get the max one as the active segment file.
		// 按序打开段文件，将段文件ID最大的段文件作为活跃段文件
		sort.Ints(segmentIDs)

		for i, segId := range segmentIDs {
			segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt, uint32(segId))
			if err != nil {
				return nil, err
			}
			if i == len(segmentIDs)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[segment.id] = segment
			}
		}
	}

	// only start the sync operation if the SyncInterval is greater than 0.
	// 这段代码的作用是根据配置选项决定是否启用一个后台同步任务
	// SyncInterval 是一个配置选项，表示同步操作的时间间隔。如果它大于 0，意味着需要定期执行同步操作。
	// 当 SyncInterval 大于 0 时，创建一个新的定时器 syncTicker，
	// 每隔 SyncInterval 的时间间隔就会向其通道 C 发送一个时间信号，这个定时器用于触发定期的同步操作。
	// goroutine 中使用了一个无限循环，持续监听两个通道事件
	// todo 没太看懂下面这段代码
	if wal.options.SyncInterval > 0 {
		wal.syncTicker = time.NewTicker(wal.options.SyncInterval)
		go func() {
			for {
				select {
				// 当定时器触发时，从其通道 C 接收时间信号，然后调用 wal.Sync() 方法进行同步操作。
				case <-wal.syncTicker.C:
					_ = wal.Sync()
				// 监听 wal.closeC 通道。如果收到关闭信号，说明需要停止同步任务
				// 此时调用 wal.syncTicker.Stop() 停止定时器，然后退出 goroutine（return）。
				case <-wal.closeC:
					wal.syncTicker.Stop()
					return
				}
			}
		}()
	}

	return wal, nil
}

// SegmentFileName returns the file name of a segment file.
// SegmentFileName 返回段文件的文件名
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

// OpenNewActiveSegment opens a new segment file
// and sets it as the active segment file.
// It is used when even the active segment file is not full,
// but the user wants to create a new segment file.
//
// It is now used by Merge operation of rosedb, not a common usage for most users.
//
// OpenNewActiveSegment 打开一个新的段文件并将其设为活跃段文件
// 它在活跃文件未满，但是用户想创建一个新的段文件时使用
// 它当前在rosedb中用于Merge操作，对大多数用户来说并不常用
func (wal *WAL) OpenNewActiveSegment() error {
	// wal实例独占锁
	wal.mu.Lock()
	defer wal.mu.Unlock()
	// sync the active segment file.
	// 当前活跃文件落盘
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	// create a new segment file and set it as the active one.
	// 创建一个新的段文件，并将其设置为活跃文件
	segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt, wal.activeSegment.id+1)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

// ActiveSegmentID returns the id of the active segment file.
// ActiveSegmentID 返回活跃段文件的ID
func (wal *WAL) ActiveSegmentID() SegmentID {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return wal.activeSegment.id
}

// IsEmpty returns whether the WAL is empty.
// Only there is only one empty active segment file, which means the WAL is empty.
// IsEmpty 返回WAL是否为空
// 当只有一个空的活跃文件时，WAL为空
func (wal *WAL) IsEmpty() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return len(wal.olderSegments) == 0 && wal.activeSegment.Size() == 0
}

// SetIsStartupTraversal This is only used if the WAL is during startup traversal.
// Such as rosedb/lotusdb startup, so it's not a common usage for most users.
// And notice that if you set it to true, only one reader can read the data from the WAL
// (Single Thread).
//
// SetIsStartupTraversal 该函数仅在WAL处于启动遍历时使用
// 要看一下这个函数在哪使用
func (wal *WAL) SetIsStartupTraversal(v bool) {
	for _, seg := range wal.olderSegments {
		seg.isStartupTraversal = v
	}
	wal.activeSegment.isStartupTraversal = v
}

// NewReaderWithMax returns a new reader for the WAL,
// and the reader will only read the data from the segment file
// whose id is less than or equal to the given segId.
//
// It is now used by the Merge operation of rosedb, not a common usage for most users.
//
// NewReaderWithMax 返回一个新用于WAL实例的reader，并且该reader只会从小于等于给定segId的段文件中读取数据
// 它现在由 rosedb 的合并操作使用，对大多数用户来说并不常用。
func (wal *WAL) NewReaderWithMax(segId SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// get all segment readers.
	// 获取所有段文件reader
	var segmentReaders []*segmentReader
	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewReader()
			segmentReaders = append(segmentReaders, reader)
		}
	}
	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewReader()
		segmentReaders = append(segmentReaders, reader)
	}

	// sort the segment readers by segment id.
	// 基于段文件ID对reader进行升序排序
	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

// NewReaderWithStart returns a new reader for the WAL,
// and the reader will only read the data from the segment file
// whose position is greater than or equal to the given position.
//
// NewReaderWithStart 返回一个新用于WAL实例的reader，并且该reader只会从大于等于给定segId的段文件中读取数据
func (wal *WAL) NewReaderWithStart(startPos *ChunkPosition) (*Reader, error) {
	if startPos == nil {
		return nil, errors.New("start position is nil")
	}
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	reader := wal.NewReader()
	for {
		// skip the segment readers whose id is less than the given position's segment id.
		// 跳过段文件ID小于给定段文件ID的reader
		if reader.CurrentSegmentId() < startPos.SegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		// skip the chunk whose position is less than the given position.
		// 跳过位置小于给定位置的chunk块。
		currentPos := reader.CurrentChunkPosition()
		if currentPos.BlockNumber >= startPos.BlockNumber &&
			currentPos.ChunkOffset >= startPos.ChunkOffset {
			break
		}
		// call Next to find again.
		// 调用Next函数找到下一个chunk块
		if _, _, err := reader.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return reader, nil
}

// NewReader returns a new reader for the WAL.
// It will iterate all segment files and read all data from them.
// NewReader 返回一个用于WAL实例的新reader
func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

// Next returns the next chunk data and its position in the WAL.
// If there is no data, io.EOF will be returned.
//
// The position can be used to read the data from the segment file.
//
// Next 返回下一个chunk块在WAL实例中的数据和位置，如果没有数据则会返回io.EOF
// position可以被用于从段文件中读取数据
// Reader和segmentReader的区别：
//
//	Reader：用于对外提供reader读取器，判断currentReader是否发生越界，如果发生说明后续已无数据
//	segmentReaders：用于对内提供reader读取器，实际上真正的去获取下一条数据，但每次只能读取当前段文件
//					切换段文件的时机是读取到io.EOF时，切换段文件的动作由Reader完成
func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	// 如果当前reader的ID比reader数组的长度还大，说明后续已经没有数据，返回io.EOF
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}

	// 读取下一条chunk数据，调用segmentReader的Next
	data, position, err := r.segmentReaders[r.currentReader].Next()
	// 如果错误为io.EOF，说明读到段文件末尾，当前readerID+1，然后再次调用reader的Next
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

// SkipCurrentSegment skips the current segment file
// when reading the WAL.
//
// It is now used by the Merge operation of rosedb, not a common usage for most users.
//
// SkipCurrentSegment 在读取WAL时跳过当前段文件
// 它现在由 rosedb 的合并操作使用，对大多数用户来说并不常用。
func (r *Reader) SkipCurrentSegment() {
	r.currentReader++
}

// CurrentSegmentId returns the id of the current segment file when reading the WAL.
// CurrentSegmentId 返回读取WAL时的当前段文件ID
func (r *Reader) CurrentSegmentId() SegmentID {
	return r.segmentReaders[r.currentReader].segment.id
}

// CurrentChunkPosition returns the position of the current chunk data
// CurrentChunkPosition 返回当前chunk数据的位置
func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentId:   reader.segment.id,
		BlockNumber: reader.blockNumber,
		ChunkOffset: reader.chunkOffset,
	}
}

// ClearPendingWrites clear pendingWrite and reset pendingSize
// ClearPendingWrites 清理待写数组并重置待写数组大小
func (wal *WAL) ClearPendingWrites() {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	wal.pendingSize = 0
	wal.pendingWrites = wal.pendingWrites[:0]
}

// PendingWrites add data to wal.pendingWrites and wait for batch write.
// If the data in pendingWrites exceeds the size of one segment,
// it will return a 'ErrPendingSizeTooLarge' error and clear the pendingWrites.
//
// PendingWrites 把数据添加到待写数组并等待批次写入
// 如果 pendingWrites 中的数据超过一个段文件的大小，系统将返回 “ErrPendingSizeTooLarge ”错误并清除
// pendingWrites
func (wal *WAL) PendingWrites(data []byte) {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	size := wal.maxDataWriteSize(int64(len(data)))
	wal.pendingSize += size
	wal.pendingWrites = append(wal.pendingWrites, data)
}

// rotateActiveSegment create a new segment file and replace the activeSegment.
// rotateActiveSegment 创建一个新的段文件，然后替换活跃段文件
func (wal *WAL) rotateActiveSegment() error {
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	wal.bytesWrite = 0
	segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt,
		wal.activeSegment.id+1)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

// WriteAll write wal.pendingWrites to WAL and then clear pendingWrites,
// it will not sync the segment file based on wal.options, you should call Sync() manually.
// WriteAll 将待写数组写入WAL，并清理待写数组
// 不会根据 wal.options 同步段文件，你应该手动调用 Sync()
func (wal *WAL) WriteAll() ([]*ChunkPosition, error) {
	if len(wal.pendingWrites) == 0 {
		return make([]*ChunkPosition, 0), nil
	}

	wal.mu.Lock()
	defer func() {
		wal.ClearPendingWrites()
		wal.mu.Unlock()
	}()

	// if the pending size is still larger than segment size, return error
	// 如果待写数组大小大于段文件，返回错误
	if wal.pendingSize > wal.options.SegmentSize {
		return nil, ErrPendingSizeTooLarge
	}

	// if the active segment file is full, sync it and create a new one.
	// 如果活跃段文件已满，将活跃段文件落盘，然后创建一个新的段文件
	if wal.activeSegment.Size()+wal.pendingSize > wal.options.SegmentSize {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	// write all data to the active segment file.
	// 返回写入数据在 WAL 中的位置数组，如果有错误则返回错误信息。
	positions, err := wal.activeSegment.writeAll(wal.pendingWrites)
	if err != nil {
		return nil, err
	}

	// 返回数据的ChunkPosition
	return positions, nil
}

// Write writes the data to the WAL.
// Actually, it writes the data to the active segment file.
// It returns the position of the data in the WAL, and an error if any.
//
// Write 将数据写入WAL
// 实际上，将数据写入活跃段文件
// 它返回写入数据在 WAL 中的位置，如果有错误则返回错误信息。
func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// 如果写入数据大于段文件，返回错误
	if int64(len(data))+chunkHeaderSize > wal.options.SegmentSize {
		return nil, ErrValueTooLarge
	}
	// if the active segment file is full, sync it and create a new one.
	// 如果活跃段文件已满，将活跃段文件落盘，然后创建一个新的段文件
	if wal.isFull(int64(len(data))) {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	// write the data to the active segment file.
	// 将数据写入活跃段文件
	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	// update the bytesWrite field.
	// 更新bytesWrite字段
	wal.bytesWrite += position.ChunkSize

	// sync the active segment file if needed.
	// 根据options中的Sync配置项，决定是否落盘
	var needSync = wal.options.Sync
	// 如果needSync为false，且BytesPerSync(每次同步N字节) > 0，则令needSync为true，否则为false
	if !needSync && wal.options.BytesPerSync > 0 {
		needSync = wal.bytesWrite >= wal.options.BytesPerSync
	}
	// 如果needSync为true，则活跃段文件执行落盘，并将bytesWrite更新为0
	if needSync {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
	}

	// 返回写入数据在 WAL 中的位置，如果有错误则返回错误信息
	return position, nil
}

// Read reads the data from the WAL according to the given position.
// Read 根据给定的position从WAL中读取数据
func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// find the segment file according to the position.
	// 根据position查找段文件
	var segment *segment
	if pos.SegmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.SegmentId]
	}

	// 如果segment为nil，则返回段文件找不到的错误
	if segment == nil {
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentId, wal.options.SegmentFileExt)
	}

	// read the data from the segment file.
	// 调用segment.Read
	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

// Close closes the WAL.
// Close 关闭WAL
func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// todo：不知道这里在干什么
	// 这段代码的目的是安全地关闭 wal.closeC 通道，同时避免重复关闭通道引发的错误。
	// 因为在 Go 中，如果尝试关闭一个已经关闭的通道，会导致程序出现异常（panic）。
	// 通过 select 语句先检测通道是否已经关闭，若未关闭则进行关闭操作，
	// 确保通道在程序执行过程中被正确、安全地关闭一次。
	select {
	case <-wal.closeC:
		// channel is already closed
	default:
		close(wal.closeC)
	}

	// close all segment files.
	// 关闭历史段文件
	for _, segment := range wal.olderSegments {
		if err := segment.Close(); err != nil {
			return err
		}
		wal.renameIds = append(wal.renameIds, segment.id)
	}
	wal.olderSegments = nil

	wal.renameIds = append(wal.renameIds, wal.activeSegment.id)
	// close the active segment file.
	// 关闭活跃段文件
	return wal.activeSegment.Close()
}

// Delete deletes all segment files of the WAL.
// Delete 删除WAL中的所有段文件
func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// delete all segment files.
	// 删除所有段文件
	for _, segment := range wal.olderSegments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil

	// delete the active segment file.
	// 删除活跃段文件
	return wal.activeSegment.Remove()
}

// Sync syncs the active segment file to stable storage like disk.
// Sync 会将活跃段文件同步到磁盘等稳定存储器中。
func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

// RenameFileExt renames all segment files' extension name.
// It is now used by the Merge operation of loutsdb, not a common usage for most users.
// RenameFileExt 修改所有段文件的扩展名
// 它现在由 loutsdb 的合并操作使用，对大多数用户来说并不常用。
func (wal *WAL) RenameFileExt(ext string) error {
	if !strings.HasPrefix(ext, ".") {
		return fmt.Errorf("segment file extension must start with '.'")
	}
	wal.mu.Lock()
	defer wal.mu.Unlock()

	renameFile := func(id SegmentID) error {
		oldName := SegmentFileName(wal.options.DirPath, wal.options.SegmentFileExt, id)
		newName := SegmentFileName(wal.options.DirPath, ext, id)
		return os.Rename(oldName, newName)
	}

	for _, id := range wal.renameIds {
		if err := renameFile(id); err != nil {
			return err
		}
	}

	wal.options.SegmentFileExt = ext
	return nil
}

// isFull：段文件是否已满
// 如果段文件大小加上当前传入的数据大小>配置项中的段文件大小，则认为段文件已满
func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+wal.maxDataWriteSize(delta) > wal.options.SegmentSize
}

// maxDataWriteSize calculate the possible maximum size.
// the maximum size = max padding + dataSize + (num_block + 1) * headerSize
//
// maxDataWriteSize 计算可能的最大值
// max padding = chunkHeaderSize
// num_block = size/blockSize (这里实际上是整除)
// 如果size<blockSize，num_block = 0；如果size>blockSize， num_block = 向下取整（size/blockSize）
// the maximum size = chunkHeaderSize + size + (num_block + 1) * headerSize
func (wal *WAL) maxDataWriteSize(size int64) int64 {
	return chunkHeaderSize + size + (size/blockSize+1)*chunkHeaderSize
}
