package operations

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/lib/readers"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	multithreadChunkSize     = 64 << 10
	multithreadChunkSizeMask = multithreadChunkSize - 1
)

// An offsetWriter maps writes at offset base to offset base+off in the underlying writer.
//
// Modified from the go source code. Can be replaced with
// io.OffsetWriter when we no longer need to support go1.19
type offsetWriter struct {
	w   io.WriterAt
	off int64 // the current offset
}

// newOffsetWriter returns an offsetWriter that writes to w
// starting at offset off.
func newOffsetWriter(w io.WriterAt, off int64) *offsetWriter {
	return &offsetWriter{w, off}
}

func (o *offsetWriter) Write(p []byte) (n int, err error) {
	n, err = o.w.WriteAt(p, o.off)
	o.off += int64(n)
	return
}

// Return a boolean as to whether we should use multi thread copy for
// this transfer
func doMultiThreadCopy(ctx context.Context, f fs.Fs, src fs.Object) bool {
	ci := fs.GetConfig(ctx)

	// Disable multi thread if...

	// ...it isn't configured
	if ci.MultiThreadStreams <= 1 {
		return false
	}
	// ...if the source doesn't support it
	if src.Fs().Features().NoMultiThreading {
		return false
	}
	// ...size of object is less than cutoff
	if src.Size() < int64(ci.MultiThreadCutoff) {
		return false
	}
	// ...destination doesn't support it
	dstFeatures := f.Features()
	if dstFeatures.OpenChunkWriter == nil && dstFeatures.OpenWriterAt == nil {
		return false
	}
	// ...if --multi-thread-streams not in use and source and
	// destination are both local
	if !ci.MultiThreadSet && dstFeatures.IsLocal && src.Fs().Features().IsLocal {
		return false
	}
	return true
}

// state for a multi-thread copy
type multiThreadCopyState struct {
	ctx       context.Context
	partSize  int64
	size      int64
	src       fs.Object
	acc       *accounting.Account
	streams   int
	numChunks int
}

// Copy a single stream into place
func (mc *multiThreadCopyState) copyStream(ctx context.Context, stream int, writer fs.ChunkWriter) (err error) {
	defer func() {
		if err != nil {
			fs.Debugf(mc.src, "multi-thread copy: stream %d/%d failed: %v", stream+1, mc.numChunks, err)
		}
	}()
	start := int64(stream) * mc.partSize
	if start >= mc.size {
		return nil
	}
	end := start + mc.partSize
	if end > mc.size {
		end = mc.size
	}

	fs.Debugf(mc.src, "multi-thread copy: stream %d/%d (%d-%d) size %v starting", stream+1, mc.numChunks, start, end, fs.SizeSuffix(end-start))

	rc, err := Open(ctx, mc.src, &fs.RangeOption{Start: start, End: end - 1})
	if err != nil {
		fs.Debugf(mc.src, "multi-thread copy: failed to open source", err)
		return fmt.Errorf("multi-thread copy: failed to open source: %w", err)
	}
	defer fs.CheckClose(rc, &err)

	bytesWritten, err := writer.WriteChunk(stream, readers.NewRepeatableReader(rc))
	if err != nil {
		return err
	}
	// TODO: This approach will only report progress when a chunk finishes writing
	// An alternative is to create a wrapper that implements ReadSeeker, such as:
	// accR := readerAccounter{
	//		reader: readers.NewRepeatableReader(rc),
	//		acc:    mc.acc,
	//	}
	//	func (r readerAccounter) Read(p []byte) (n int, err error) {
	//		n, err = r.reader.Read(p)
	//		errAccRead := r.acc.AccountRead(n)
	//		if errAccRead != nil {
	//			return -1, errAccRead
	//		}
	//		return n, err
	//	}
	//
	//	func (r readerAccounter) Seek(offset int64, whence int) (n int64, err error) {
	//		n, err = r.reader.Seek(offset, whence)
	//		return n, err
	//	}
	// however to ensure report works correctly writer.WriteChunk must only reads once (not the case with S3 and md5 calculation...)
	errAccRead := mc.acc.AccountRead(int(bytesWritten))
	if errAccRead != nil {
		fs.Debugf(mc.src, "multi-thread copy: account reader err", errAccRead)
		return errAccRead
	}

	fs.Debugf(mc.src, "multi-thread copy: stream %d/%d (%d-%d) size %v finished", stream+1, mc.numChunks, start, end, bytesWritten)
	return nil
}

// Given a file size and number of streams
// it returns a partSize and update number of streams, so that partSize * streams >= size
func calculateChunks(size int64, streams int) (int64, int) {
	partSize := size / int64(streams)
	// Round partition size up so partSize * streams >= size
	if (size % int64(streams)) != 0 {
		partSize++
	}
	// round partSize up to nearest multithreadChunkSize boundary
	partSize = (partSize + multithreadChunkSizeMask) &^ multithreadChunkSizeMask
	// recalculate number of streams
	streams = int(size / partSize)
	// round streams up so partSize * streams >= size
	if (size % partSize) != 0 {
		streams++
	}
	return partSize, streams
}

// Copy src to (f, remote) using streams download threads. It tries to use the OpenChunkWriter feature
// and if that's not available it creates an adapter using OpenWriterAt
func multiThreadCopy(ctx context.Context, f fs.Fs, remote string, src fs.Object, streams int, tr *accounting.Transfer) (newDst fs.Object, err error) {
	openChunkWriter := f.Features().OpenChunkWriter
	ci := fs.GetConfig(ctx)
	if openChunkWriter == nil {
		openWriterAt := f.Features().OpenWriterAt
		if openWriterAt == nil {
			return nil, errors.New("multi-part copy: neither OpenChunkWriter nor OpenWriterAt supported")
		}
		openChunkWriter = openChunkWriterFromOpenWriterAt(openWriterAt, int64(ci.MultiThreadWriteBufferSize), streams, f)
	}

	if src.Size() < 0 {
		return nil, fmt.Errorf("multi-thread copy: can't copy unknown sized file")
	}
	if src.Size() == 0 {
		return nil, fmt.Errorf("multi-thread copy: can't copy zero sized file")
	}

	g, gCtx := errgroup.WithContext(ctx)
	chunkSize, chunkWriter, err := openChunkWriter(ctx, remote, src)
	numChunks := src.Size() / chunkSize
	if src.Size()%chunkSize != 0 {
		numChunks++
	}
	mc := &multiThreadCopyState{
		ctx:       gCtx,
		size:      src.Size(),
		src:       src,
		partSize:  chunkSize,
		streams:   streams,
		numChunks: int(numChunks),
	}

	if err != nil {
		return nil, fmt.Errorf("multipart copy: failed to open chunk writer: %w", err)
	}

	// Make accounting
	mc.acc = tr.Account(ctx, nil)

	fs.Debugf(src, "Starting multi-thread copy with %d parts of size %v with %v parallel streams", mc.numChunks, fs.SizeSuffix(mc.partSize), mc.streams)
	sem := semaphore.NewWeighted(int64(mc.streams))
	for chunk := 0; chunk < mc.numChunks; chunk++ {
		fs.Debugf(src, "Acquiring semaphore...")
		if err := sem.Acquire(ctx, 1); err != nil {
			fs.Errorf(src, "Failed to acquire semaphore: %v", err)
			break
		}
		currChunk := chunk
		g.Go(func() (err error) {
			defer sem.Release(1)
			return mc.copyStream(gCtx, currChunk, chunkWriter)
		})
	}

	err = g.Wait()
	fs.Debugf(f, "HERE!!!")
	closeErr := chunkWriter.Close()
	if err != nil {
		fs.Debugf(f, "multi-thread copy: error when waiting for chunks: %w", err)
		return nil, err
	}
	if closeErr != nil {
		fs.Debugf(f, "multi-thread copy: error closing chunk writer: %w", closeErr)
		return nil, fmt.Errorf("multi-thread copy: failed to close object after copy: %w", closeErr)
	}

	obj, err := f.NewObject(ctx, remote)
	if err != nil {
		fs.Debugf(f, "multi-thread copy: error creating new object: %w", err)
		return nil, fmt.Errorf("multi-thread copy: failed to find object after copy: %w", err)
	}

	err = obj.SetModTime(ctx, src.ModTime(ctx))
	switch err {
	case nil, fs.ErrorCantSetModTime, fs.ErrorCantSetModTimeWithoutDelete:
	default:
		fs.Debugf(f, "multi-thread copy: failed to set modification time: %w", err)
		return nil, fmt.Errorf("multi-thread copy: failed to set modification time: %w", err)
	}

	fs.Debugf(src, "Finished multi-thread copy with %d parts of size %v", mc.numChunks, fs.SizeSuffix(mc.partSize))
	return obj, nil
}

type writerAtChunkWriter struct {
	ctx             context.Context
	remote          string
	size            int64
	writerAt        fs.WriterAtCloser
	chunkSize       int64
	chunks          int
	writeBufferSize int64
	f               fs.Fs
}

func (w writerAtChunkWriter) WriteChunk(chunkNumber int, reader io.ReadSeeker) (int64, error) {
	fs.Debugf(w.remote, "writing chunk %v", chunkNumber)

	bytesToWrite := w.chunkSize
	if chunkNumber == (w.chunks-1) && w.size%w.chunkSize != 0 {
		bytesToWrite = w.size % w.chunkSize
	}

	var writer io.Writer = newOffsetWriter(w.writerAt, int64(chunkNumber)*w.chunkSize)
	if w.writeBufferSize > 0 {
		writer = bufio.NewWriterSize(writer, int(w.writeBufferSize))
	}
	n, err := io.Copy(writer, reader)
	if err != nil {
		return -1, err
	}
	if n != bytesToWrite {
		return -1, fmt.Errorf("expected to write %v bytes for chunk %v, but wrote %v bytes", bytesToWrite, chunkNumber, n)
	}
	// if we were buffering, flush do disk
	switch w := writer.(type) {
	case *bufio.Writer:
		er2 := w.Flush()
		if er2 != nil {
			return -1, fmt.Errorf("multipart copy: flush failed: %w", err)
		}
	}
	return n, nil
}

func (w writerAtChunkWriter) Close() error {
	return w.writerAt.Close()
}

func (w writerAtChunkWriter) Abort() error {
	obj, err := w.f.NewObject(w.ctx, w.remote)
	if err != nil {
		return fmt.Errorf("multi-thread copy: failed to find temp file when aborting chunk writer: %w", err)
	}
	return obj.Remove(w.ctx)
}

func openChunkWriterFromOpenWriterAt(openWriterAt func(ctx context.Context, remote string, size int64) (fs.WriterAtCloser, error), writeBufferSize int64, streams int, f fs.Fs) func(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (chunkSizeResult int64, writer fs.ChunkWriter, err error) {
	return func(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (chunkSizeResult int64, writer fs.ChunkWriter, err error) {
		writerAt, err := openWriterAt(ctx, src.Remote(), src.Size())
		if err != nil {
			return -1, nil, err
		}

		if writeBufferSize > 0 {
			fs.Debugf(src.Remote(), "multi-thread copy: write buffer set to %v", writeBufferSize)
		}

		chunkSize, numStreams := calculateChunks(src.Size(), streams)
		chunkWriter := &writerAtChunkWriter{
			ctx:             ctx,
			remote:          remote,
			size:            src.Size(),
			chunkSize:       chunkSize,
			chunks:          numStreams,
			writerAt:        writerAt,
			writeBufferSize: writeBufferSize,
			f:               f,
		}

		return chunkSize, chunkWriter, nil
	}
}
