// Copyright © 2026 Afero authors.
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

// Package afero provides types and methods for interacting with the filesystem,
// as an abstraction layer.

// This file adds QuotaFs, an explicit total byte quota wrapper for any Fs.
//
// Design
//
// QuotaFs wraps an underlying Fs without changing its interface: QuotaFs is
// itself an Fs and every operation is delegated to the wrapped Fs after the
// quota accounting has been updated. The quota limits the total length of
// regular files, so only the growth caused by Create, Write, WriteAt and
// Truncate is charged.
//
// Accounting model
//
// The wrapper keeps, per tracked file, the number of bytes that have been
// paid for (the "charged" high-water mark) and a global total of charged
// bytes. Before a write, the growth is projected from the current file
// length and the write's end position; if the projection would exceed the
// quota, the operation is rejected without touching the underlying file.
// Otherwise the growth is reserved up front. After the underlying call, the
// reservation is reconciled with the actual file length (read back from the
// handle): a short write or a failed write is only kept charged for the
// bytes that actually landed, and the rest is refunded. Overwrites within
// the charged length are free, and a sparse WriteAt is charged by the
// resulting file length.
//
// All accounting is guarded by a single mutex that is held across the
// underlying call, so the reserve and the commit are atomic with respect to
// other handles: concurrent handles on the same file can never exceed the
// quota together, and the accounting does not have to be deferred to Close
// (which would let concurrent writes overrun the quota before any close).
//
// Deleted files
//
// When a file is removed while handles are still open, the refund is deferred
// until the last handle closes: the bytes stay charged in the meantime. A
// failed remove does not refund. A Truncate shrink refunds immediately on
// success.
//
// Symbolic links
//
// Symbolic links are rejected at this entry point: Open, Create and OpenFile
// on a path reported as a symlink by the wrapped Fs's LstatIfPossible return
// ErrSymlinkNotAllowed, so writes cannot bypass the accounting by pointing at
// a different file. The wrapper does not expose the wrapped Fs, so there is
// no path through the wrapper that reaches the underlying file directly.

package afero

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ErrQuotaExceeded is returned when an operation would make a file grow beyond
// the total byte quota enforced by a QuotaFs. When this error is returned the
// underlying filesystem has not been touched.
var ErrQuotaExceeded = errors.New("quota exceeded")

// ErrSymlinkNotAllowed is returned when a path that is a symbolic link is
// accessed through a QuotaFs. Symbolic links are rejected at this entry point
// so that writes cannot bypass the quota accounting by pointing at a
// different file.
var ErrSymlinkNotAllowed = errors.New("symbolic links are not allowed")

// quotaFile tracks the charged bytes for a single file. All fields are
// protected by the QuotaFs mutex.
type quotaFile struct {
	charged     int64 // bytes currently charged (the high-water mark that has been paid for)
	openHandles int   // number of open handles referencing this file
	deleted     bool  // the namespace file has been removed; the refund is deferred until the last handle closes
}

// QuotaFs wraps an Fs and enforces a total byte quota on the length of files.
// Only the growth caused by Create, Write, WriteAt and Truncate is charged:
// overwrites within the already charged length are free, sparse WriteAt is
// charged by the resulting file length and a short write or a failed write is
// only charged for the bytes that actually landed.
//
// The underlying Fs interface is not changed: QuotaFs is an Fs itself and
// delegates every operation to the wrapped Fs after updating its accounting.
type QuotaFs struct {
	source Fs
	quota  int64

	mu    sync.Mutex
	used  int64
	files map[string]*quotaFile
}

// NewQuotaFs wraps source and enforces the given total byte quota. The sizes
// of the regular files that already exist in source are counted towards the
// quota at creation time.
func NewQuotaFs(source Fs, quota int64) (*QuotaFs, error) {
	q := &QuotaFs{source: source, quota: quota, files: make(map[string]*quotaFile)}
	if err := q.scan(); err != nil {
		return nil, err
	}
	return q, nil
}

// Quota returns the total byte quota.
func (q *QuotaFs) Quota() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.quota
}

// Used returns the number of bytes currently charged.
func (q *QuotaFs) Used() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}

// Available returns the number of bytes that can still be charged.
func (q *QuotaFs) Available() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.quota - q.used
}

func (q *QuotaFs) Name() string { return "QuotaFs" }

// scan counts the existing regular files and populates the per-file state.
func (q *QuotaFs) scan() error {
	return q.scanDir(string(filepath.Separator))
}

func (q *QuotaFs) scanDir(dir string) error {
	entries, err := ReadDir(q.source, dir)
	if err != nil {
		return err
	}
	for _, fi := range entries {
		full := filepath.Join(dir, fi.Name())
		if lf, _, lerr := q.lstat(full); lerr == nil {
			fi = lf
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			// Symlinks are not counted and not followed.
			continue
		}
		if fi.IsDir() {
			if err := q.scanDir(full); err != nil {
				return err
			}
			continue
		}
		if fi.Mode().IsRegular() {
			q.files[qNormPath(full)] = &quotaFile{charged: fi.Size()}
			q.used += fi.Size()
		}
	}
	return nil
}

func (q *QuotaFs) lstat(name string) (os.FileInfo, bool, error) {
	if lstater, ok := q.source.(Lstater); ok {
		return lstater.LstatIfPossible(name)
	}
	fi, err := q.source.Stat(name)
	return fi, false, err
}

func (q *QuotaFs) isSymlink(name string) bool {
	fi, _, err := q.lstat(name)
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeSymlink != 0
}

func (q *QuotaFs) rejectSymlink(name, op string) error {
	if q.isSymlink(name) {
		return &os.PathError{Op: op, Path: name, Err: ErrSymlinkNotAllowed}
	}
	return nil
}

// lockedFile returns the tracked state for name, creating it on the fly for a
// file that exists but was not tracked (e.g. it appeared before the wrapper
// was created). The caller must hold q.mu.
func (q *QuotaFs) lockedFile(name string) *quotaFile {
	nname := qNormPath(name)
	if qf := q.files[nname]; qf != nil {
		return qf
	}
	qf := &quotaFile{}
	if fi, err := q.source.Stat(name); err == nil && fi.Mode().IsRegular() {
		qf.charged = fi.Size()
	}
	q.files[nname] = qf
	return qf
}

func (q *QuotaFs) wrap(sf File, qf *quotaFile, name string, writable bool) *quotaFileHandle {
	h := &quotaFileHandle{File: sf, fs: q, qf: qf, name: name, writable: writable}
	if pos, err := sf.Seek(0, io.SeekCurrent); err == nil {
		h.off = pos
	}
	return h
}

// qNormPath normalizes a path the same way the MemMapFs does, so that the
// per-file state keyed by the wrapper matches the paths used by the wrapped
// filesystem.
func qNormPath(p string) string {
	if p == "" {
		return ""
	}
	p = filepath.Clean(p)
	if p == "." || p == ".." {
		return string(filepath.Separator)
	}
	if !filepath.IsAbs(p) {
		p = string(filepath.Separator) + p
	}
	return p
}

func (q *QuotaFs) Create(name string) (File, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.rejectSymlink(name, "create"); err != nil {
		return nil, err
	}
	sf, err := q.source.Create(name)
	if err != nil {
		return nil, err
	}
	nname := qNormPath(name)
	// Create replaces any previous file with an empty one.
	if old := q.files[nname]; old != nil {
		if old.openHandles == 0 {
			q.used -= old.charged
		} else {
			// The old file is gone from the namespace but its handles are still
			// open: defer the refund until the last one closes.
			old.deleted = true
		}
		delete(q.files, nname)
	}
	qf := &quotaFile{}
	q.files[nname] = qf
	qf.openHandles++
	return q.wrap(sf, qf, nname, true), nil
}

func (q *QuotaFs) Open(name string) (File, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.rejectSymlink(name, "open"); err != nil {
		return nil, err
	}
	sf, err := q.source.Open(name)
	if err != nil {
		return nil, err
	}
	qf := q.lockedFile(name)
	qf.openHandles++
	return q.wrap(sf, qf, qNormPath(name), false), nil
}

func (q *QuotaFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.rejectSymlink(name, "open"); err != nil {
		return nil, err
	}
	_, statErr := q.source.Stat(name)
	existed := statErr == nil
	truncate := flag&os.O_TRUNC != 0
	// We handle O_TRUNC ourselves so that shrinking is accounted for.
	sf, err := q.source.OpenFile(name, flag&^os.O_TRUNC, perm)
	if err != nil {
		return nil, err
	}
	nname := qNormPath(name)
	var qf *quotaFile
	if existed {
		qf = q.lockedFile(name)
	} else {
		qf = &quotaFile{}
		q.files[nname] = qf
	}
	if truncate && existed {
		if err := sf.Truncate(0); err != nil {
			sf.Close()
			return nil, err
		}
		q.used -= qf.charged
		qf.charged = 0
	}
	qf.openHandles++
	writable := flag&(os.O_WRONLY|os.O_RDWR) != 0
	return q.wrap(sf, qf, nname, writable), nil
}

func (q *QuotaFs) Remove(name string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	nname := qNormPath(name)
	qf := q.files[nname]
	if qf == nil {
		// Not tracked: start tracking it so a successful remove can refund it.
		if fi, err := q.source.Stat(name); err == nil && fi.Mode().IsRegular() {
			qf = &quotaFile{charged: fi.Size()}
			q.files[nname] = qf
		}
	}
	if err := q.source.Remove(name); err != nil {
		// A failed remove must not refund anything.
		return err
	}
	if qf != nil {
		if qf.openHandles == 0 {
			q.used -= qf.charged
		} else {
			qf.deleted = true
		}
		delete(q.files, nname)
	}
	return nil
}

func (q *QuotaFs) RemoveAll(path string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	npath := qNormPath(path)
	if err := q.source.RemoveAll(path); err != nil {
		return err
	}
	prefix := npath + string(filepath.Separator)
	for k, qf := range q.files {
		if k == npath || strings.HasPrefix(k, prefix) {
			if qf.openHandles == 0 {
				q.used -= qf.charged
			} else {
				qf.deleted = true
			}
			delete(q.files, k)
		}
	}
	return nil
}

func (q *QuotaFs) Rename(oldname, newname string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	nold := qNormPath(oldname)
	nnew := qNormPath(newname)
	if nold == nnew {
		return q.source.Rename(oldname, newname)
	}
	if err := q.source.Rename(oldname, newname); err != nil {
		return err
	}
	if qf := q.files[nold]; qf != nil {
		// A single file moved.
		delete(q.files, nold)
		if nqf := q.files[nnew]; nqf != nil && nqf != qf {
			// The destination was overwritten.
			if nqf.openHandles == 0 {
				q.used -= nqf.charged
			} else {
				nqf.deleted = true
			}
		}
		q.files[nnew] = qf
	} else {
		// A directory moved: rewrite the descendant keys.
		oldPrefix := nold + string(filepath.Separator)
		newPrefix := nnew + string(filepath.Separator)
		for k, v := range q.files {
			if strings.HasPrefix(k, oldPrefix) {
				delete(q.files, k)
				q.files[newPrefix+strings.TrimPrefix(k, oldPrefix)] = v
			}
		}
	}
	return nil
}

func (q *QuotaFs) Stat(name string) (os.FileInfo, error) {
	return q.source.Stat(name)
}

func (q *QuotaFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	return q.lstat(name)
}

func (q *QuotaFs) Mkdir(name string, perm os.FileMode) error {
	return q.source.Mkdir(name, perm)
}

func (q *QuotaFs) MkdirAll(path string, perm os.FileMode) error {
	return q.source.MkdirAll(path, perm)
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

// quotaFileHandle wraps a File and charges the growth of the underlying file
// to the QuotaFs total.
type quotaFileHandle struct {
	File
	fs       *QuotaFs
	qf       *quotaFile
	name     string
	writable bool
	off      int64
	closed   bool
}

func (h *quotaFileHandle) Write(p []byte) (int, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.closed {
		return 0, ErrFileClosed
	}
	if !h.writable {
		return 0, syscall.EPERM
	}
	if len(p) == 0 {
		return h.File.Write(p)
	}
	qf := h.qf
	current := qf.charged
	projected := h.off + int64(len(p))
	reserved := projected - current
	if reserved > 0 {
		if h.fs.used+reserved > h.fs.quota {
			return 0, ErrQuotaExceeded // the underlying file is not touched
		}
		h.fs.used += reserved
		qf.charged = projected
	}
	n, err := h.File.Write(p)
	h.off += int64(n)
	h.reconcileLocked(current, reserved)
	return n, err
}

func (h *quotaFileHandle) WriteAt(p []byte, off int64) (int, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.closed {
		return 0, ErrFileClosed
	}
	if !h.writable {
		return 0, syscall.EPERM
	}
	if off < 0 {
		return 0, ErrOutOfRange
	}
	if len(p) == 0 {
		return h.File.WriteAt(p, off)
	}
	qf := h.qf
	current := qf.charged
	projected := off + int64(len(p))
	reserved := projected - current
	if reserved > 0 {
		if h.fs.used+reserved > h.fs.quota {
			return 0, ErrQuotaExceeded // the underlying file is not touched
		}
		h.fs.used += reserved
		qf.charged = projected
	}
	n, err := h.File.WriteAt(p, off)
	// WriteAt does not move the file offset.
	h.reconcileLocked(current, reserved)
	return n, err
}

func (h *quotaFileHandle) Truncate(size int64) error {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.closed {
		return ErrFileClosed
	}
	if !h.writable {
		return syscall.EPERM
	}
	return h.truncateLocked(size)
}

// truncateLocked changes the file length, accounting for the growth or the
// shrink. The caller must hold h.fs.mu.
func (h *quotaFileHandle) truncateLocked(size int64) error {
	qf := h.qf
	current := qf.charged
	if size > current {
		growth := size - current
		if h.fs.used+growth > h.fs.quota {
			return ErrQuotaExceeded // the underlying file is not touched
		}
		h.fs.used += growth
		qf.charged = size
	}
	err := h.File.Truncate(size)
	if err != nil {
		if size > current {
			// The underlying truncate failed: undo the reservation.
			h.fs.used -= size - current
			qf.charged = current
		}
		return err
	}
	if size < current {
		// The shrink succeeded: refund the difference now.
		h.fs.used -= current - size
		qf.charged = size
	}
	return nil
}

func (h *quotaFileHandle) WriteString(s string) (int, error) {
	return h.Write([]byte(s))
}

func (h *quotaFileHandle) Read(p []byte) (int, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.closed {
		return 0, ErrFileClosed
	}
	n, err := h.File.Read(p)
	h.off += int64(n)
	return n, err
}

func (h *quotaFileHandle) Seek(offset int64, whence int) (int64, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.closed {
		return 0, ErrFileClosed
	}
	pos, err := h.File.Seek(offset, whence)
	if err == nil {
		h.off = pos
	}
	return pos, err
}

func (h *quotaFileHandle) Close() error {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.closed {
		return ErrFileClosed
	}
	if err := h.File.Close(); err != nil {
		return err
	}
	h.closed = true
	qf := h.qf
	qf.openHandles--
	if qf.openHandles == 0 && qf.deleted {
		// The last handle on a deleted file is closed: refund the bytes that
		// were kept reserved until now.
		h.fs.used -= qf.charged
	}
	return nil
}

// reconcileLocked makes the actual file length match the reservation after a
// write. The write may have been short or may have failed: only the bytes
// that actually landed are kept charged. The caller must hold h.fs.mu.
func (h *quotaFileHandle) reconcileLocked(current, reserved int64) {
	if reserved <= 0 {
		return
	}
	fi, err := h.File.Stat()
	if err != nil {
		// The actual size cannot be verified; keep the reservation.
		return
	}
	actual := fi.Size()
	if actual < current {
		actual = current
	}
	actualGrowth := actual - current
	if actualGrowth < 0 {
		actualGrowth = 0
	}
	if actualGrowth < reserved {
		h.fs.used -= reserved - actualGrowth
		h.qf.charged = current + actualGrowth
	} else if actualGrowth > reserved {
		h.fs.used += actualGrowth - reserved
		h.qf.charged = actual
	}
}

var _ Fs = (*QuotaFs)(nil)
var _ Lstater = (*QuotaFs)(nil)
