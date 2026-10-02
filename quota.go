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
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

var (
	// ErrQuotaExceeded is returned by QuotaFs when an operation would grow
	// the total file length beyond the configured quota. When this error is
	// returned the underlying filesystem is never touched.
	ErrQuotaExceeded = errors.New("afero: quota exceeded")

	// ErrQuotaLink is returned by QuotaFs for symbolic links and any other
	// non-regular, non-directory entries. NewQuotaFs refuses to wrap a
	// filesystem that already contains such entries, and the wrapper never
	// creates them nor opens them for writing, so quota accounting cannot be
	// bypassed through link indirection.
	ErrQuotaLink = errors.New("afero: quota fs does not allow links")
)

var (
	_ Fs         = (*QuotaFs)(nil)
	_ Lstater    = (*QuotaFs)(nil)
	_ Linker     = (*QuotaFs)(nil)
	_ LinkReader = (*QuotaFs)(nil)
)

// QuotaFs is an Fs wrapper that enforces an explicit quota on the total
// number of bytes stored in regular files of the wrapped filesystem.
//
// The quota only limits growth of file length caused by Create, Write,
// WriteAt, WriteString and Truncate; the wrapped Fs interface is unchanged
// and read-only operations are passed through untouched.
//
// Accounting rules:
//   - NewQuotaFs walks the source filesystem and charges the length of every
//     existing regular file against the quota. Links (and any other
//     non-regular, non-directory entries) make NewQuotaFs fail; the wrapper
//     also refuses to create or write through links, and never exposes the
//     wrapped Fs, so the quota cannot be bypassed.
//   - Overwriting existing bytes is free: only growth beyond the current
//     file length is charged. Sparse writes (WriteAt beyond the end of the
//     file) are charged by the resulting new file length.
//   - All handles of the same file share one reservation/accounting record.
//     Quota is reserved before the underlying call and only the actually
//     written growth is committed, so short writes or failures of the
//     underlying filesystem only charge real growth, and rejections never
//     touch the underlying filesystem.
//   - Truncate that shrinks a file refunds its quota immediately. A failed
//     Remove refunds nothing. A successful Remove refunds immediately only
//     when no handle is open; otherwise the refund is deferred until the
//     last open handle is Closed. Accounting always happens at write time,
//     never deferred to Close, so concurrent handles cannot jointly exceed
//     the quota.
type QuotaFs struct {
	source Fs

	mu    sync.Mutex
	quota int64
	used  int64 // committed bytes + bytes reserved by in-flight operations
	files map[string]*quotaState
}

// quotaState is the shared accounting record for one file generation. All
// handles of the same file point at the same record.
type quotaState struct {
	size     int64 // committed (charged) file length
	reserved int64 // bytes reserved by in-flight writes (already in used)
	handles  int   // number of open handles
	deleted  bool  // removed from the namespace; refund deferred to last Close
}

// NewQuotaFs wraps source with a total byte quota. It walks source and
// charges all existing regular files against the quota. If source contains
// links or other non-regular, non-directory entries, NewQuotaFs fails with
// an error wrapping ErrQuotaLink.
func NewQuotaFs(source Fs, quota int64) (*QuotaFs, error) {
	q := &QuotaFs{
		source: source,
		quota:  quota,
		files:  make(map[string]*quotaState),
	}
	err := Walk(source, FilePathSeparator, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return &os.PathError{Op: "quota-scan", Path: p, Err: ErrQuotaLink}
		}
		key := quotaKey(p)
		st := q.files[key]
		if st == nil {
			st = &quotaState{}
			q.files[key] = st
		}
		st.size += info.Size()
		q.used += info.Size()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return q, nil
}

// quotaKey normalizes a path so that all operations naming the same file
// share one accounting record.
func quotaKey(name string) string {
	return path.Clean("/" + filepath.ToSlash(name))
}

// Quota returns the configured total byte quota.
func (q *QuotaFs) Quota() int64 { return q.quota }

// Used returns the currently charged bytes, including reservations held by
// in-flight write operations.
func (q *QuotaFs) Used() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}

// Name of this filesystem wrapper.
func (q *QuotaFs) Name() string { return "QuotaFs" }

// rejectLink refuses write access through symbolic links so the quota cannot
// be bypassed via link indirection.
func (q *QuotaFs) rejectLink(name string) error {
	fi, _, err := q.LstatIfPossible(name)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !fi.Mode().IsRegular() && !fi.IsDir() {
		return &os.PathError{Op: "open", Path: name, Err: ErrQuotaLink}
	}
	return nil
}

// register attaches a freshly opened underlying file to its accounting
// record, creating or truncating the record as needed.
func (q *QuotaFs) register(name string, f File, trunc bool) File {
	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		// Directories (and unstattable files) are not metered.
		return &quotaFile{File: f, fs: q}
	}
	key := quotaKey(name)
	q.mu.Lock()
	st := q.files[key]
	if st == nil {
		// Unknown to the accounting (e.g. created behind the wrapper's
		// back): adopt its current length honestly.
		size := fi.Size()
		if trunc {
			size = 0
		}
		st = &quotaState{size: size}
		q.files[key] = st
		q.used += size
	} else if trunc {
		// Create/O_TRUNC reset the file to zero length: refund immediately.
		q.used -= st.size
		st.size = 0
	}
	st.handles++
	q.mu.Unlock()
	return &quotaFile{File: f, fs: q, state: st}
}

func (q *QuotaFs) Create(name string) (File, error) {
	if err := q.rejectLink(name); err != nil {
		return nil, err
	}
	f, err := q.source.Create(name)
	if err != nil {
		return nil, err
	}
	// Create truncates to zero length.
	return q.register(name, f, true), nil
}

func (q *QuotaFs) Open(name string) (File, error) {
	f, err := q.source.Open(name)
	if err != nil {
		return nil, err
	}
	return q.register(name, f, false), nil
}

func (q *QuotaFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		if err := q.rejectLink(name); err != nil {
			return nil, err
		}
	}
	f, err := q.source.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	wrapped := q.register(name, f, flag&os.O_TRUNC != 0)
	if qf, ok := wrapped.(*quotaFile); ok && flag&os.O_APPEND != 0 {
		qf.appendMode = true
	}
	return wrapped, nil
}

func (q *QuotaFs) Remove(name string) error {
	// A failed removal refunds nothing: the bytes are still there.
	if err := q.source.Remove(name); err != nil {
		return err
	}
	key := quotaKey(name)
	q.mu.Lock()
	if st, ok := q.files[key]; ok {
		delete(q.files, key)
		st.deleted = true
		if st.handles == 0 {
			q.used -= st.size
			st.size = 0
		}
		// Otherwise the refund is deferred to the last Close.
	}
	q.mu.Unlock()
	return nil
}

func (q *QuotaFs) RemoveAll(name string) error {
	// Collect the regular files about to disappear so their records can be
	// released (or marked deleted) after a successful removal.
	var doomed []string
	_ = Walk(q.source, name, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // let the underlying RemoveAll decide
		}
		if info.Mode().IsRegular() {
			doomed = append(doomed, quotaKey(p))
		}
		return nil
	})
	if err := q.source.RemoveAll(name); err != nil {
		return err
	}
	q.mu.Lock()
	for _, key := range doomed {
		if st, ok := q.files[key]; ok {
			delete(q.files, key)
			st.deleted = true
			if st.handles == 0 {
				q.used -= st.size
				st.size = 0
			}
		}
	}
	q.mu.Unlock()
	return nil
}

func (q *QuotaFs) Rename(oldname, newname string) error {
	if err := q.source.Rename(oldname, newname); err != nil {
		return err
	}
	oldKey, newKey := quotaKey(oldname), quotaKey(newname)
	q.mu.Lock()
	if st, ok := q.files[newKey]; ok {
		// The rename target was overwritten: release its record.
		delete(q.files, newKey)
		st.deleted = true
		if st.handles == 0 {
			q.used -= st.size
			st.size = 0
		}
	}
	if st, ok := q.files[oldKey]; ok {
		delete(q.files, oldKey)
		q.files[newKey] = st
	}
	q.mu.Unlock()
	return nil
}

// SymlinkIfPossible is denied: links could be used to bypass quota
// accounting, so the wrapper never creates them.
func (q *QuotaFs) SymlinkIfPossible(oldname, newname string) error {
	return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: ErrQuotaLink}
}

func (q *QuotaFs) ReadlinkIfPossible(name string) (string, error) {
	if lr, ok := q.source.(LinkReader); ok {
		return lr.ReadlinkIfPossible(name)
	}
	return "", &os.PathError{Op: "readlink", Path: name, Err: ErrNoReadlink}
}

func (q *QuotaFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	if ls, ok := q.source.(Lstater); ok {
		return ls.LstatIfPossible(name)
	}
	fi, err := q.source.Stat(name)
	return fi, false, err
}

func (q *QuotaFs) Stat(name string) (os.FileInfo, error) { return q.source.Stat(name) }

func (q *QuotaFs) Mkdir(name string, perm os.FileMode) error { return q.source.Mkdir(name, perm) }

func (q *QuotaFs) MkdirAll(name string, perm os.FileMode) error {
	return q.source.MkdirAll(name, perm)
}

func (q *QuotaFs) Chmod(name string, mode os.FileMode) error {
	return q.source.Chmod(name, mode)
}

func (q *QuotaFs) Chown(name string, uid, gid int) error {
	return q.source.Chown(name, uid, gid)
}

func (q *QuotaFs) Chtimes(name string, atime, mtime time.Time) error {
	return q.source.Chtimes(name, atime, mtime)
}

// quotaFile wraps a File and meters every growth of the file length against
// the quota of its QuotaFs. Handles without a state (directories) are pure
// pass-throughs.
type quotaFile struct {
	File
	fs         *QuotaFs
	state      *quotaState // shared by all handles of the same file
	appendMode bool
}

// reserve checks and reserves quota for growing the file to end bytes. It
// returns the reserved growth, or ErrQuotaExceeded without touching the
// underlying filesystem.
func (f *quotaFile) reserve(end int64) (int64, error) {
	q := f.fs
	q.mu.Lock()
	defer q.mu.Unlock()
	growth := end - f.state.size
	if growth <= 0 {
		return 0, nil // pure overwrite: free
	}
	if q.used+growth > q.quota {
		return 0, &os.PathError{Op: "write", Path: filepath.ToSlash(f.Name()), Err: ErrQuotaExceeded}
	}
	q.used += growth
	f.state.reserved += growth
	return growth, nil
}

// commit drops the reservation and commits only the growth that actually
// happened (off+n versus the shared committed size). Short writes and
// failures of the underlying filesystem therefore only charge real growth.
func (f *quotaFile) commit(off int64, n int, reserved int64) {
	q := f.fs
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used -= reserved
	f.state.reserved -= reserved
	if f.appendMode {
		// O_APPEND always extends the file by exactly what was written.
		f.state.size += int64(n)
		q.used += int64(n)
		return
	}
	if end := off + int64(n); end > f.state.size {
		q.used += end - f.state.size
		f.state.size = end
	}
}

// writeOffset returns the offset a Write would use, without writing.
func (f *quotaFile) writeOffset() (int64, error) {
	if f.appendMode {
		f.fs.mu.Lock()
		defer f.fs.mu.Unlock()
		return f.state.size, nil
	}
	return f.File.Seek(0, io.SeekCurrent)
}

func (f *quotaFile) Write(p []byte) (int, error) {
	if f.state == nil || len(p) == 0 {
		return f.File.Write(p)
	}
	off, err := f.writeOffset()
	if err != nil {
		return 0, err
	}
	reserved, err := f.reserve(off + int64(len(p)))
	if err != nil {
		return 0, err
	}
	n, werr := f.File.Write(p)
	f.commit(off, n, reserved)
	return n, werr
}

func (f *quotaFile) WriteString(s string) (int, error) {
	if f.state == nil || len(s) == 0 {
		return f.File.WriteString(s)
	}
	off, err := f.writeOffset()
	if err != nil {
		return 0, err
	}
	reserved, err := f.reserve(off + int64(len(s)))
	if err != nil {
		return 0, err
	}
	n, werr := f.File.WriteString(s)
	f.commit(off, n, reserved)
	return n, werr
}

func (f *quotaFile) WriteAt(p []byte, off int64) (int, error) {
	if f.state == nil || len(p) == 0 {
		return f.File.WriteAt(p, off)
	}
	reserved, err := f.reserve(off + int64(len(p)))
	if err != nil {
		return 0, err
	}
	n, werr := f.File.WriteAt(p, off)
	f.commit(off, n, reserved)
	return n, werr
}

func (f *quotaFile) Truncate(size int64) error {
	if f.state == nil {
		return f.File.Truncate(size)
	}
	q := f.fs
	q.mu.Lock()
	var reserved int64
	if size > f.state.size {
		reserved = size - f.state.size
		if q.used+reserved > q.quota {
			q.mu.Unlock()
			return &os.PathError{Op: "truncate", Path: filepath.ToSlash(f.Name()), Err: ErrQuotaExceeded}
		}
		q.used += reserved
		f.state.reserved += reserved
	}
	q.mu.Unlock()

	err := f.File.Truncate(size)

	q.mu.Lock()
	if reserved > 0 {
		q.used -= reserved
		f.state.reserved -= reserved
	}
	if err == nil {
		// Commit the real new length: growth charges, shrinkage refunds.
		q.used += size - f.state.size
		f.state.size = size
	}
	q.mu.Unlock()
	return err
}

func (f *quotaFile) Close() error {
	err := f.File.Close()
	if f.state != nil {
		q := f.fs
		q.mu.Lock()
		f.state.handles--
		if f.state.handles == 0 && f.state.deleted {
			// Last handle of a removed file: release the deferred refund.
			q.used -= f.state.size
			f.state.size = 0
		}
		q.mu.Unlock()
		f.state = nil
	}
	return err
}
