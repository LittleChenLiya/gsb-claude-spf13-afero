// Copyright © 2026.
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
	"errors"
	"io"
	"os"
	"strconv"
	"syscall"
	"time"
)

// zeroFillChunkSize is the largest single zero-filling write issued by the
// default filler. Filling a hole is done in bounded pieces so that a failure
// can be reported together with the number of zero bytes that were actually
// materialized.
const zeroFillChunkSize = 1 << 20 // 1 MiB

// FillError reports that explicitly materializing the zero-filled hole between
// the previous end of a file and a write (or truncation) past that end did not
// finish.
//
// Filled is the number of hole bytes written successfully; the caller's payload
// is not written when this error is returned. It is reported instead of a faked
// atomic success so callers can determine the real progress made on the file.
type FillError struct {
	// Op is "write" or "truncate".
	Op string
	// Path is the file on which the fill failed.
	Path string
	// Filled is the count of zero bytes materialized before the failure.
	Filled int64
	// Err is the underlying failure.
	Err error
}

func (e *FillError) Error() string {
	return "consistentwritefs: zero-fill " + e.Op + " " + e.Path +
		": filled " + strconv.FormatInt(e.Filled, 10) + " bytes: " + e.Err.Error()
}

func (e *FillError) Unwrap() error { return e.Err }

// filler materializes the n zero bytes of a hole starting at absolute offset
// start. It returns the number of zero bytes actually written and an error.
// Fills use WriteAt and therefore do not move the handle's read/write cursor.
// The default implementation performs bounded chunked writes; tests may
// substitute a failing filler to verify progress reporting.
type filler func(f File, start, n int64) (int64, error)

// defaultFiller materializes n zero bytes beginning at start in chunks of
// zeroFillChunkSize. A short write by the backend is reported honestly together
// with the bytes that did land rather than being treated as success. The zero
// buffer is only ever passed to WriteAt, which must not retain or modify it, so
// it is shared.
var zeroFillBuf = make([]byte, zeroFillChunkSize)

func defaultFiller(f File, start, n int64) (int64, error) {
	var written int64
	for written < n {
		todo := n - written
		if todo > int64(len(zeroFillBuf)) {
			todo = int64(len(zeroFillBuf))
		}
		m, err := f.WriteAt(zeroFillBuf[:todo], start+written)
		written += int64(m)
		if err != nil {
			return written, err
		}
		if int64(m) < todo {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// maxInt64 mirrors math.MaxInt64 without a version-dependent import.
const maxInt64 = int64(^uint64(0) >> 1)

var (
	errOffsetOverflow = errors.New("offset overflows int64")
	errNegativeOffset = errors.New("negative offset")
)

// ConsistentWriteFs wraps another Fs and makes past-end writes and extending
// truncations explicitly consistent:
//
//   - Writing (or WriteAt) past the current end first materializes the hole as
//     real zero bytes, so the gap reads back as zeros on every backend.
//   - Truncate that grows a file extends it with an explicit zero fill and
//     never alters the existing prefix.
//   - Offset arithmetic is checked for overflow and negative positions before
//     touching the backend.
//   - Short writes and failed fills report the bytes that really completed
//     instead of pretending the operation succeeded atomically.
//
// Behavior of the wrapped backend is otherwise unchanged; this adapter only
// layers the guarantees above on top of it.
type ConsistentWriteFs struct {
	source Fs
}

// Compile-time assertions for the interfaces the wrapper honors.
var (
	_ Fs         = (*ConsistentWriteFs)(nil)
	_ Lstater    = (*ConsistentWriteFs)(nil)
	_ Linker     = (*ConsistentWriteFs)(nil)
	_ LinkReader = (*ConsistentWriteFs)(nil)
	_ File       = (*consistentWriteFile)(nil)
)

// NewConsistentWriteFs returns an explicit consistent-write adapter around
// source.
func NewConsistentWriteFs(source Fs) *ConsistentWriteFs {
	return &ConsistentWriteFs{source: source}
}

func (c *ConsistentWriteFs) Name() string {
	return "ConsistentWriteFs(" + c.source.Name() + ")"
}

func (c *ConsistentWriteFs) Create(name string) (File, error) {
	f, err := c.source.Create(name)
	if err != nil || f == nil {
		return f, err
	}
	return newConsistentWriteFile(f, true), nil
}

func (c *ConsistentWriteFs) Open(name string) (File, error) {
	f, err := c.source.Open(name)
	if err != nil || f == nil {
		return f, err
	}
	return newConsistentWriteFile(f, true), nil
}

func (c *ConsistentWriteFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := c.source.OpenFile(name, flag, perm)
	if err != nil || f == nil {
		return f, err
	}
	return newConsistentWriteFile(f, true), nil
}

func (c *ConsistentWriteFs) Mkdir(name string, perm os.FileMode) error {
	return c.source.Mkdir(name, perm)
}

func (c *ConsistentWriteFs) MkdirAll(path string, perm os.FileMode) error {
	return c.source.MkdirAll(path, perm)
}

func (c *ConsistentWriteFs) Remove(name string) error { return c.source.Remove(name) }

func (c *ConsistentWriteFs) RemoveAll(path string) error { return c.source.RemoveAll(path) }

func (c *ConsistentWriteFs) Rename(oldname, newname string) error {
	return c.source.Rename(oldname, newname)
}

func (c *ConsistentWriteFs) Stat(name string) (os.FileInfo, error) { return c.source.Stat(name) }

func (c *ConsistentWriteFs) Chmod(name string, mode os.FileMode) error {
	return c.source.Chmod(name, mode)
}

func (c *ConsistentWriteFs) Chown(name string, uid, gid int) error {
	return c.source.Chown(name, uid, gid)
}

func (c *ConsistentWriteFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return c.source.Chtimes(name, atime, mtime)
}

func (c *ConsistentWriteFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	if lsf, ok := c.source.(Lstater); ok {
		return lsf.LstatIfPossible(name)
	}
	fi, err := c.source.Stat(name)
	return fi, false, err
}

func (c *ConsistentWriteFs) SymlinkIfPossible(oldname, newname string) error {
	if linker, ok := c.source.(Linker); ok {
		return linker.SymlinkIfPossible(oldname, newname)
	}
	return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: ErrNoSymlink}
}

func (c *ConsistentWriteFs) ReadlinkIfPossible(name string) (string, error) {
	if reader, ok := c.source.(LinkReader); ok {
		return reader.ReadlinkIfPossible(name)
	}
	return "", &os.PathError{Op: "readlink", Path: name, Err: ErrNoReadlink}
}

// consistentWriteFile is the File handle returned by ConsistentWriteFs. It
// delegates reads and metadata operations to the wrapped handle while routing
// writes and extending truncations through explicit zero filling.
type consistentWriteFile struct {
	f     File
	owned bool
	fill  filler
	pos   int64
}

// NewConsistentWriteFile adapts a File the caller already holds into a
// consistent-write handle. The borrowed file is not closed when the returned
// handle is closed: ownership stays with the caller. Handles produced by
// ConsistentWriteFs own their underlying handle and do close it.
func NewConsistentWriteFile(f File) File {
	return newConsistentWriteFile(f, false)
}

func newConsistentWriteFile(f File, owned bool) *consistentWriteFile {
	return &consistentWriteFile{f: f, owned: owned}
}

func (c *consistentWriteFile) filler() filler {
	if c.fill != nil {
		return c.fill
	}
	return defaultFiller
}

func (c *consistentWriteFile) Name() string { return c.f.Name() }

func (c *consistentWriteFile) Stat() (os.FileInfo, error) { return c.f.Stat() }

func (c *consistentWriteFile) Sync() error { return c.f.Sync() }

// Read delegates to the wrapped handle and keeps the adapter cursor in step,
// so a later Write without an intervening Seek continues right after the bytes
// just read.
func (c *consistentWriteFile) Read(p []byte) (int, error) {
	n, err := c.f.Read(p)
	c.advance(int64(n))
	return n, err
}

func (c *consistentWriteFile) ReadAt(p []byte, off int64) (int, error) {
	return c.f.ReadAt(p, off)
}

func (c *consistentWriteFile) Readdir(count int) ([]os.FileInfo, error) {
	return c.f.Readdir(count)
}

func (c *consistentWriteFile) Readdirnames(n int) ([]string, error) {
	return c.f.Readdirnames(n)
}

// Close closes the wrapped handle only when this adapter owns it. A handle
// built via NewConsistentWriteFile borrows the file, so closing it leaves the
// borrowed file usable by its actual owner.
func (c *consistentWriteFile) Close() error {
	if !c.owned {
		return nil
	}
	return c.f.Close()
}

// Seek validates the resulting position before delegating. Negative positions
// and positions that overflow int64 are rejected without moving the underlying
// cursor.
func (c *consistentWriteFile) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		if (offset > 0 && offset > maxInt64-c.pos) || (offset < 0 && offset < -c.pos) {
			return c.pos, &os.PathError{Op: "seek", Path: c.f.Name(), Err: errOffsetOverflow}
		}
		target = c.pos + offset
	case io.SeekEnd:
		fi, err := c.f.Stat()
		if err != nil {
			return c.pos, err
		}
		size := fi.Size()
		if (offset > 0 && offset > maxInt64-size) || (offset < 0 && offset < -size) {
			return c.pos, &os.PathError{Op: "seek", Path: c.f.Name(), Err: errOffsetOverflow}
		}
		target = size + offset
	default:
		return c.pos, &os.PathError{Op: "seek", Path: c.f.Name(), Err: syscall.EINVAL}
	}
	if target < 0 {
		return c.pos, &os.PathError{Op: "seek", Path: c.f.Name(), Err: errNegativeOffset}
	}
	off, err := c.f.Seek(target, io.SeekStart)
	if err != nil {
		return c.pos, err
	}
	c.pos = off
	return off, nil
}

// Write materializes any hole before the current position as explicit zero
// bytes, then writes p. The returned n counts only payload bytes from p: a fill
// failure yields n == 0 with a *FillError carrying the true fill progress, and
// a short payload write yields the real payload length with io.ErrShortWrite.
func (c *consistentWriteFile) Write(p []byte) (int, error) {
	if len(p) == 0 {
		n, err := c.f.Write(p)
		if err == nil {
			c.advance(int64(n))
		}
		return n, err
	}
	if c.pos < 0 {
		return 0, &os.PathError{Op: "write", Path: c.f.Name(), Err: errNegativeOffset}
	}
	if c.pos > maxInt64-int64(len(p)) {
		return 0, &os.PathError{Op: "write", Path: c.f.Name(), Err: errOffsetOverflow}
	}
	n, err := c.writeAtFrom(p, c.pos)
	if err == nil {
		c.advance(int64(n))
		// writeAtFrom writes via WriteAt, which does not move the backend
		// cursor; re-align it to the advanced position so a following Read
		// continues right after the payload.
		_, _ = c.f.Seek(c.pos, io.SeekStart)
	}
	return n, err
}

// WriteString mirrors Write for string payloads.
func (c *consistentWriteFile) WriteString(s string) (int, error) {
	return c.Write([]byte(s))
}

// WriteAt behaves like Write but at an absolute offset and never moves the
// handle cursor.
func (c *consistentWriteFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, &os.PathError{Op: "writeat", Path: c.f.Name(), Err: errNegativeOffset}
	}
	if len(p) > 0 && off > maxInt64-int64(len(p)) {
		return 0, &os.PathError{Op: "writeat", Path: c.f.Name(), Err: errOffsetOverflow}
	}
	if len(p) == 0 {
		return c.f.WriteAt(p, off)
	}
	return c.writeAtFrom(p, off)
}

func (c *consistentWriteFile) advance(n int64) { c.pos += n }

// writeAtFrom performs the fill-then-write sequence at absolute offset off.
// Filling uses WriteAt so it does not disturb the handle cursor; the payload
// is written at off after any hole is materialized.
func (c *consistentWriteFile) writeAtFrom(p []byte, off int64) (int, error) {
	fi, err := c.f.Stat()
	if err != nil {
		return 0, err
	}
	size := fi.Size()

	if off > size {
		hole := off - size
		filled, ferr := c.filler()(c.f, size, hole)
		if ferr != nil {
			return 0, &FillError{Op: "write", Path: c.f.Name(), Filled: filled, Err: ferr}
		}
	}

	n, werr := c.f.WriteAt(p, off)
	if werr != nil {
		return n, werr
	}
	if n < len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

// Truncate grows or shrinks the file to size. A growth explicitly zero-fills
// the extension (via WriteAt, leaving the cursor untouched) and leaves the
// existing prefix byte-for-byte untouched; a fill failure reports the real
// extension progress via *FillError.
func (c *consistentWriteFile) Truncate(size int64) error {
	if size < 0 {
		return &os.PathError{Op: "truncate", Path: c.f.Name(), Err: errNegativeOffset}
	}
	fi, err := c.f.Stat()
	if err != nil {
		return err
	}
	cur := fi.Size()
	if size <= cur {
		return c.f.Truncate(size)
	}

	growth := size - cur
	filled, ferr := c.filler()(c.f, cur, growth)
	if ferr != nil {
		return &FillError{Op: "truncate", Path: c.f.Name(), Filled: filled, Err: ferr}
	}
	return nil
}
