// Copyright © 2026 Steve Francia <spf@spf13.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package afero

import (
	"io"
	"math"
	"os"
	"time"
)

// defaultFillChunkSize is the default number of zero bytes written per
// WriteAt call when filling a hole. A smaller value makes fill progress
// observable in finer steps; a larger value is more efficient.
const defaultFillChunkSize = 4096

// ConsistentWriteFs wraps an Fs to provide explicit, consistent zero-fill
// semantics.
//
// Writes that land past the end of a file, and growth performed by Truncate,
// are explicitly zero-filled rather than relying on the underlying
// filesystem's sparse-file behaviour. This guarantees that holes read back
// as zero bytes on every backend, including backends that do not zero-fill
// extended files on their own.
//
// The adapter is opt-in: the underlying source Fs is never modified, and
// backends that already zero-fill (such as MemMapFs and most native OSes)
// keep their normal behaviour when not wrapped.
type ConsistentWriteFs struct {
	source        Fs
	fillChunkSize int
}

// ConsistentWriteFile wraps a File so that past-EOF writes and Truncate
// growth are explicitly zero-filled.
type ConsistentWriteFile struct {
	File
	fs    *ConsistentWriteFs
	owned bool
}

var (
	_ Fs   = (*ConsistentWriteFs)(nil)
	_ File = (*ConsistentWriteFile)(nil)
)

// NewConsistentWriteFs returns an Fs that explicitly zero-fills holes
// created by writes past EOF and by Truncate growth. The returned Fs wraps
// source; source itself is not modified.
func NewConsistentWriteFs(source Fs) *ConsistentWriteFs {
	return &ConsistentWriteFs{source: source, fillChunkSize: defaultFillChunkSize}
}

// WithFillChunkSize sets the chunk size used when zero-filling holes. A
// smaller chunk size makes fill progress observable in finer steps. Values
// <= 0 are ignored. The receiver is returned for chaining.
func (c *ConsistentWriteFs) WithFillChunkSize(n int) *ConsistentWriteFs {
	if n > 0 {
		c.fillChunkSize = n
	}
	return c
}

// FillChunkSize returns the chunk size used when zero-filling holes.
func (c *ConsistentWriteFs) FillChunkSize() int {
	if c.fillChunkSize <= 0 {
		return defaultFillChunkSize
	}
	return c.fillChunkSize
}

func (c *ConsistentWriteFs) Name() string { return "ConsistentWriteFs" }

func (c *ConsistentWriteFs) Create(name string) (File, error) {
	f, err := c.source.Create(name)
	if err != nil {
		return nil, err
	}
	return c.wrap(f, true), nil
}

func (c *ConsistentWriteFs) Mkdir(name string, perm os.FileMode) error {
	return c.source.Mkdir(name, perm)
}

func (c *ConsistentWriteFs) MkdirAll(path string, perm os.FileMode) error {
	return c.source.MkdirAll(path, perm)
}

func (c *ConsistentWriteFs) Open(name string) (File, error) {
	f, err := c.source.Open(name)
	if err != nil {
		return nil, err
	}
	return c.wrap(f, true), nil
}

func (c *ConsistentWriteFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := c.source.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return c.wrap(f, true), nil
}

func (c *ConsistentWriteFs) Remove(name string) error {
	return c.source.Remove(name)
}

func (c *ConsistentWriteFs) RemoveAll(path string) error {
	return c.source.RemoveAll(path)
}

func (c *ConsistentWriteFs) Rename(oldname, newname string) error {
	return c.source.Rename(oldname, newname)
}

func (c *ConsistentWriteFs) Stat(name string) (os.FileInfo, error) {
	return c.source.Stat(name)
}

func (c *ConsistentWriteFs) Chmod(name string, mode os.FileMode) error {
	return c.source.Chmod(name, mode)
}

func (c *ConsistentWriteFs) Chown(name string, uid, gid int) error {
	return c.source.Chown(name, uid, gid)
}

func (c *ConsistentWriteFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return c.source.Chtimes(name, atime, mtime)
}

// wrap wraps f in a ConsistentWriteFile. When owned is true, closing the
// wrapper closes the underlying file; when false (a borrowed file), the
// underlying file is left open.
func (c *ConsistentWriteFs) wrap(f File, owned bool) File {
	if f == nil {
		return nil
	}
	return &ConsistentWriteFile{File: f, fs: c, owned: owned}
}

// WrapFile wraps an existing File without taking ownership of it. The
// returned File's Close will not close the underlying File; the caller
// remains responsible for closing it. This is useful when a File is shared
// with another owner that must control its lifetime.
func (c *ConsistentWriteFs) WrapFile(f File) File {
	return c.wrap(f, false)
}

// Close closes the underlying file only when the wrapper owns it. A
// borrowed (non-owned) file is left open so its owner can continue to use
// it.
func (f *ConsistentWriteFile) Close() error {
	if !f.owned {
		// Borrowed file: do not close the underlying file.
		return nil
	}
	return f.File.Close()
}

// Write writes p at the current seek position, explicitly zero-filling any
// hole between the current end of file and the seek position before
// writing. On a short write or a fill failure, n reflects the actual number
// of bytes completed rather than the requested length.
func (f *ConsistentWriteFile) Write(p []byte) (int, error) {
	pos, err := f.File.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	n, err := f.WriteAt(p, pos)
	if err == nil && n > 0 {
		if _, serr := f.File.Seek(pos+int64(n), io.SeekStart); serr != nil {
			return n, serr
		}
	}
	return n, err
}

// WriteAt writes p at off, explicitly zero-filling any hole between the
// current end of file and off before writing. On a short write or a fill
// failure, n reflects the actual number of bytes completed rather than the
// requested length.
func (f *ConsistentWriteFile) WriteAt(p []byte, off int64) (int, error) {
	if err := checkWriteBounds(off, len(p)); err != nil {
		return 0, err
	}
	// Zero-length writes must not grow the file, matching the behaviour of
	// the underlying backends.
	if len(p) == 0 {
		return f.File.WriteAt(p, off)
	}
	curSize, err := f.currentSize()
	if err != nil {
		return 0, err
	}
	if off > curSize {
		filled, err := f.fillZero(curSize, off)
		if err != nil {
			// Report the actual fill progress; do not claim atomic success.
			return int(filled), err
		}
	}
	n, err := f.File.WriteAt(p, off)
	return n, err
}

// Truncate changes the size of the file. When growing, the new bytes are
// explicitly zero-filled so the original prefix is preserved and the grown
// region reads back as zero bytes.
func (f *ConsistentWriteFile) Truncate(size int64) error {
	if size < 0 {
		return ErrOutOfRange
	}
	curSize, err := f.currentSize()
	if err != nil {
		return err
	}
	if size > curSize {
		if _, err := f.fillZero(curSize, size); err != nil {
			return err
		}
	}
	return f.File.Truncate(size)
}

// WriteString writes s at the current seek position. It delegates to Write
// so that past-EOF writes are zero-filled consistently.
func (f *ConsistentWriteFile) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

// currentSize returns the current size of the underlying file.
func (f *ConsistentWriteFile) currentSize() (int64, error) {
	fi, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// fillZero writes zero bytes over [start, end) in chunks. It returns the
// number of bytes successfully filled. On a short write or an injected
// failure, the returned count is the actual progress made.
func (f *ConsistentWriteFile) fillZero(start, end int64) (int64, error) {
	if start >= end {
		return 0, nil
	}
	chunk := f.fs.FillChunkSize()
	zeros := make([]byte, chunk)
	var filled int64
	for cur := start; cur < end; {
		want := end - cur
		if want > int64(chunk) {
			want = int64(chunk)
		}
		wn, err := f.File.WriteAt(zeros[:want], cur)
		filled += int64(wn)
		cur += int64(wn)
		if err != nil {
			return filled, err
		}
		if wn < int(want) {
			return filled, io.ErrShortWrite
		}
	}
	return filled, nil
}

// checkWriteBounds validates a write at off with length length. It checks
// for a negative offset and for offset+length overflow before any
// computation is performed.
func checkWriteBounds(off int64, length int) error {
	if off < 0 {
		return &os.PathError{Op: "write", Err: ErrOutOfRange}
	}
	if length > 0 && off > math.MaxInt64-int64(length) {
		return &os.PathError{Op: "write", Err: ErrTooLarge}
	}
	return nil
}
