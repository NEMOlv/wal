package wal

import (
	"os"
	"time"
)

// Options represents the configuration options for a Write-Ahead Log (WAL).
// Options: 预写日志WAL的配置项
type Options struct {
	// DirPath specifies the directory path where the WAL segment files will be stored.
	// DirPath 指定了WAL段文件将要存储的目录路径
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	// SegmentSize 指定了每一个段文件的最大字节大小
	SegmentSize int64

	// SegmentFileExt specifies the file extension of the segment files.
	// The file extension must start with a dot ".", default value is ".SEG".
	// It is used to identify the different types of files in the directory.
	// Now it is used by rosedb to identify the segment files and hint files.
	// Not a common usage for most users.
	//
	// SegmentFileExt 指定了段文件的后缀名
	// 文件后缀必须带有点号".",默认后缀名为".SEG"
	// 该后缀用来识别目录中不同类型的文件
	// 目前用于在RoseDB中识别段文件和hint索引文件
	// 对大多数用户来说是不常用的
	SegmentFileExt string

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	//
	// Sync 是指是否通过 os 缓冲区缓存同步写入，并同步到实际磁盘上。
	// 设置同步是单次写入操作持久性的要求，但这也会导致写入速度变慢。
	// 如果Sync设置为false, 当服务器崩溃时，可能回丢失一些最近写入的内容
	// Note：如果只是进程崩溃(机器不会崩溃)，则不会丢失任何写入的内容
	// 换句话说，Sync为false与写系统调用的语义相同，Sync为true则意味着写入之后调用fsync
	Sync bool

	// BytesPerSync specifies the number of bytes to write before calling fsync.
	// BytesPerSync 指定调用 fsync 之前要写入的字节数
	BytesPerSync uint32

	// SyncInterval is the time duration in which explicit synchronization is performed.
	// If SyncInterval is zero, no periodic synchronization is performed.
	// SyncInterval 是执行定期同步的持续时间。
	// 如果SyncInterval为0，则不执行定期同步
	SyncInterval time.Duration
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        os.TempDir(),
	SegmentSize:    GB,
	SegmentFileExt: ".SEG",
	Sync:           false,
	BytesPerSync:   0,
	SyncInterval:   0,
}
