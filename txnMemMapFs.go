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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/afero/mem"
)

// versionedEntry is one node of a TxnMemMapFs. In addition to the usual
// mem.FileData payload it carries an opaque, monotonically increasing version
// which is bumped every time the node (including its parent directory
// membership) is changed.
type versionedEntry struct {
	data    *mem.FileData
	version uint64
}

func (e *versionedEntry) isDir() bool {
	return mem.GetFileInfo(e.data).IsDir()
}

// A putOp is the final, materialized representation of one overlay node that a
// transaction wants to publish into a TxnMemMapFs.
type putOp struct {
	name string
	data *mem.FileData
	// expectedVersion is the version the entry had when the transaction first
	// observed the corresponding path. Zero means the path did not exist.
	expectedVersion uint64
	// mergeDir keeps the live directory entry and merely merges the overlay
	// children into it instead of replacing the node wholesale.
	mergeDir bool
	// resurrect marks a put whose path is deleted by an earlier remove in the
	// same batch. The remove validates the old node; this put creates a fresh
	// one, so no pre-mutation version/type check applies to it.
	resurrect bool
}

// removeOp describes one whiteout a transaction wants to publish.
type removeOp struct {
	name string
	// expectedVersion is the version of the whiteout root at read time.
	expectedVersion uint64
	// expectedDescendants maps every path that was a descendant of the
	// whiteout root at read time to that entry's version.
	expectedDescendants map[string]uint64
}

// A batch is one atomic set of changes applied to a TxnMemMapFs.
type batch struct {
	puts    []putOp
	removes []removeOp
	// expect carries the recorded versions of every other path the
	// transaction read or wrote (parents, read files/directories, ...).
	expect map[string]uint64
}

// TxnMemMapFs is an in-memory filesystem whose entries carry version
// information and which can apply a whole set of changes atomically. It is the
// default backing store of TxnOverlayFs.
//
// Every mutation through the filesystem interface bumps the versions of the
// affected nodes, which makes it possible for transactions to detect that the
// base changed underneath them between their first observation and Commit().
type TxnMemMapFs struct {
	mu      sync.RWMutex
	data    map[string]*versionedEntry
	nextVer uint64
	init    sync.Once
}

// NewTxnMemMapFs creates an empty versioned in-memory filesystem.
func NewTxnMemMapFs() *TxnMemMapFs {
	t := &TxnMemMapFs{}
	t.getData()
	return t
}

var (
	_ Fs      = (*TxnMemMapFs)(nil)
	_ Lstater = (*TxnMemMapFs)(nil)
)

func (t *TxnMemMapFs) getData() map[string]*versionedEntry {
	t.init.Do(func() {
		t.data = make(map[string]*versionedEntry)
		t.nextVer = 1
		root := mem.CreateDir(FilePathSeparator)
		mem.SetMode(root, os.ModeDir|0o755)
		t.data[FilePathSeparator] = &versionedEntry{data: root, version: t.allocVersionLocked()}
	})
	return t.data
}

func (t *TxnMemMapFs) allocVersionLocked() uint64 {
	v := t.nextVer
	t.nextVer++
	return v
}

// Name returns the name of this filesystem.
func (*TxnMemMapFs) Name() string { return "TxnMemMapFs" }

func (t *TxnMemMapFs) bumpLocked(e *versionedEntry) uint64 {
	e.version = t.allocVersionLocked()
	return e.version
}

func (t *TxnMemMapFs) openLocked(name string) (*versionedEntry, error) {
	e, ok := t.getData()[name]
	if !ok {
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
	}
	return e, nil
}

// EntryVersion returns the current version of the named path and whether the
// path exists. It is the read side of the optimistic-concurrency protocol
// used by TxnOverlayFs.
func (t *TxnMemMapFs) EntryVersion(name string) (uint64, bool) {
	name = normalizePath(name)
	t.mu.RLock()
	defer t.mu.RUnlock()
	if e, ok := t.getData()[name]; ok {
		return e.version, true
	}
	return 0, false
}

// DescendantVersions returns the current versions of every strict descendant
// of the named directory.
func (t *TxnMemMapFs) DescendantVersions(name string) map[string]uint64 {
	name = normalizePath(name)
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]uint64)
	if name == FilePathSeparator {
		for p, e := range t.getData() {
			if p != FilePathSeparator {
				out[p] = e.version
			}
		}
		return out
	}
	prefix := name + FilePathSeparator
	for p, e := range t.getData() {
		if strings.HasPrefix(p, prefix) {
			out[p] = e.version
		}
	}
	return out
}

// FileDataForCopy returns a fresh independent snapshot of the named node's
// bytes and metadata. TxnOverlayFs uses it when copying a base node up into
// its private transaction layer.
func (t *TxnMemMapFs) FileDataForCopy(name string) (*mem.FileData, error) {
	name = normalizePath(name)
	t.mu.RLock()
	e, ok := t.getData()[name]
	t.mu.RUnlock()
	if !ok {
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
	}
	return cloneFileData(e.data), nil
}

func cloneFileData(src *mem.FileData) *mem.FileData {
	fi := mem.GetFileInfo(src)
	var dst *mem.FileData
	if fi.IsDir() {
		dst = mem.CreateDir("")
	} else {
		dst = mem.CreateFile("")
	}
	mem.ChangeFileName(dst, mem.FullName(src))
	mem.SetMode(dst, fi.Mode())
	mem.SetModTime(dst, fi.ModTime())
	mem.SetUID(dst, mem.GetUID(src))
	mem.SetGID(dst, mem.GetGID(src))
	if !fi.IsDir() {
		h := mem.NewFileHandle(dst)
		srcH := mem.NewReadOnlyFileHandle(src)
		_, _ = io.Copy(h, srcH)
		_ = srcH.Close()
		_ = h.Close()
	}
	return dst
}

// Create creates a file in the filesystem, returning the file and an error.
func (t *TxnMemMapFs) Create(name string) (File, error) {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()

	file := mem.CreateFile(name)
	mem.SetMode(file, 0o666)
	t.getData()[name] = &versionedEntry{data: file, version: t.allocVersionLocked()}
	t.addToParentLocked(name, file)
	return t.newWritableHandleLocked(file), nil
}

// Mkdir creates a directory in the filesystem.
func (t *TxnMemMapFs) Mkdir(name string, perm os.FileMode) error {
	perm &= chmodBits
	name = normalizePath(name)

	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.getData()[name]; ok {
		return &os.PathError{Op: "mkdir", Path: name, Err: ErrFileExists}
	}
	item := mem.CreateDir(name)
	mem.SetMode(item, os.ModeDir|perm)
	t.getData()[name] = &versionedEntry{data: item, version: t.allocVersionLocked()}
	t.addToParentLocked(name, item)
	return nil
}

// MkdirAll creates a directory path and all parents that do not exist yet.
func (t *TxnMemMapFs) MkdirAll(path string, perm os.FileMode) error {
	perm &= chmodBits
	path = normalizePath(path)
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.getData()[path]; ok {
		return nil
	}
	cur := FilePathSeparator
	if path != FilePathSeparator {
		for _, p := range strings.Split(strings.TrimPrefix(path, FilePathSeparator), FilePathSeparator) {
			if cur == FilePathSeparator {
				cur += p
			} else {
				cur += FilePathSeparator + p
			}
			if _, ok := t.getData()[cur]; !ok {
				item := mem.CreateDir(cur)
				mem.SetMode(item, os.ModeDir|0o755)
				t.getData()[cur] = &versionedEntry{data: item, version: t.allocVersionLocked()}
				t.addToParentLocked(cur, item)
			}
		}
	}
	return nil
}

// addToParentLocked registers fd in its parent's directory listing, creating
// intermediate parent directories when necessary. Callers must hold t.mu.
func (t *TxnMemMapFs) addToParentLocked(name string, fd *mem.FileData) {
	if name == FilePathSeparator {
		return
	}
	parent := filepath.Dir(name)
	pe, ok := t.getData()[parent]
	if !ok {
		pitem := mem.CreateDir(parent)
		mem.SetMode(pitem, os.ModeDir|0o755)
		pe = &versionedEntry{data: pitem, version: t.allocVersionLocked()}
		t.getData()[parent] = pe
		t.addToParentLocked(parent, pitem)
	}
	base := filepath.Base(name)
	mem.InitializeDir(pe.data)
	for _, c := range mem.DirFiles(pe.data) {
		if filepath.Base(mem.FullName(c)) == base {
			mem.RemoveFromMemDir(pe.data, c)
		}
	}
	mem.AddToMemDir(pe.data, fd)
	t.bumpLocked(pe)
}

func (t *TxnMemMapFs) removeFromParentLocked(name string) bool {
	if name == FilePathSeparator {
		return false
	}
	parent := filepath.Dir(name)
	pe, ok := t.getData()[parent]
	if !ok {
		return false
	}
	base := filepath.Base(name)
	for _, c := range mem.DirFiles(pe.data) {
		if filepath.Base(mem.FullName(c)) == base {
			mem.RemoveFromMemDir(pe.data, c)
			t.bumpLocked(pe)
			return true
		}
	}
	return false
}

// Open opens a file read-only.
func (t *TxnMemMapFs) Open(name string) (File, error) {
	name = normalizePath(name)
	t.mu.RLock()
	e, err := t.openLocked(name)
	if err != nil {
		t.mu.RUnlock()
		return nil, err
	}
	if mem.GetFileInfo(e.data).Mode().Perm()&0o444 == 0 {
		t.mu.RUnlock()
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrPermission}
	}
	f := mem.NewReadOnlyFileHandle(e.data)
	t.mu.RUnlock()
	return f, nil
}

// OpenFile opens a file using the given flags and mode.
func (t *TxnMemMapFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	perm &= chmodBits
	name = normalizePath(name)

	t.mu.Lock()
	e, err := t.openLocked(name)
	if err == nil && flag&os.O_EXCL > 0 {
		t.mu.Unlock()
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileExists}
	}
	if err != nil {
		if !os.IsNotExist(err) || flag&os.O_CREATE == 0 {
			t.mu.Unlock()
			return nil, err
		}
		file := mem.CreateFile(name)
		mem.SetMode(file, perm)
		e = &versionedEntry{data: file, version: t.allocVersionLocked()}
		t.getData()[name] = e
		t.addToParentLocked(name, file)
		if flag == os.O_RDONLY {
			f := mem.NewReadOnlyFileHandle(file)
			t.mu.Unlock()
			return f, nil
		}
		f := t.newWritableHandleLocked(file)
		t.mu.Unlock()
		if flag&os.O_APPEND > 0 {
			_, _ = f.Seek(0, io.SeekEnd)
		}
		if flag&os.O_TRUNC > 0 && flag&(os.O_RDWR|os.O_WRONLY) > 0 {
			_ = f.Truncate(0)
		}
		return f, nil
	}

	if mem.GetFileInfo(e.data).Mode().Perm()&0o222 == 0 {
		t.mu.Unlock()
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrPermission}
	}
	if flag == os.O_RDONLY {
		f := mem.NewReadOnlyFileHandle(e.data)
		t.mu.Unlock()
		return f, nil
	}
	f := t.newWritableHandleLocked(e.data)
	t.bumpLocked(e)
	t.mu.Unlock()
	if flag&os.O_APPEND > 0 {
		_, _ = f.Seek(0, io.SeekEnd)
	}
	if flag&os.O_TRUNC > 0 && flag&(os.O_RDWR|os.O_WRONLY) > 0 {
		_ = f.Truncate(0)
	}
	return f, nil
}

// Remove removes the named file or empty directory.
func (t *TxnMemMapFs) Remove(name string) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.getData()[name]; !ok {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	t.removeFromParentLocked(name)
	delete(t.getData(), name)
	return nil
}

// RemoveAll removes a path and any children it contains.
func (t *TxnMemMapFs) RemoveAll(path string) error {
	path = normalizePath(path)
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.getData()[path]; !ok {
		return nil
	}
	t.removeFromParentLocked(path)
	if path == FilePathSeparator {
		// Recreate the root rather than leaving the fs without one.
		root := mem.CreateDir(FilePathSeparator)
		mem.SetMode(root, os.ModeDir|0o755)
		t.getData()[FilePathSeparator] = &versionedEntry{data: root, version: t.allocVersionLocked()}
		return nil
	}
	prefix := path + FilePathSeparator
	for p := range t.getData() {
		if p == path || strings.HasPrefix(p, prefix) {
			delete(t.getData(), p)
		}
	}
	return nil
}

// Rename renames a file or directory.
func (t *TxnMemMapFs) Rename(oldname, newname string) error {
	oldname = normalizePath(oldname)
	newname = normalizePath(newname)
	if oldname == newname {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	oe, ok := t.getData()[oldname]
	if !ok {
		return &os.PathError{Op: "rename", Path: oldname, Err: ErrFileNotFound}
	}
	if _, ok := t.getData()[newname]; ok {
		// POSIX: replacing a non-empty directory is an error; replacing a
		// file is permitted.
		dst := t.getData()[newname]
		if dst.isDir() && len(mem.DirFiles(dst.data)) > 0 {
			return &os.PathError{Op: "rename", Path: newname, Err: ErrFileExists}
		}
		t.removeFromParentLocked(newname)
		delete(t.getData(), newname)
	}
	t.removeFromParentLocked(oldname)

	prefix := oldname + FilePathSeparator
	type renamed struct {
		new string
		e   *versionedEntry
		old string
	}
	moved := []renamed{{old: oldname, new: newname, e: oe}}
	for p, e := range t.getData() {
		if strings.HasPrefix(p, prefix) {
			moved = append(moved, renamed{
				old: p,
				new: newname + FilePathSeparator + strings.TrimPrefix(p, prefix),
				e:   e,
			})
		}
	}
	sort.Slice(moved, func(i, j int) bool { return moved[i].new < moved[j].new })
	for _, m := range moved {
		delete(t.getData(), m.old)
		mem.ChangeFileName(m.e.data, m.new)
		t.getData()[m.new] = m.e
		t.bumpLocked(m.e)
	}
	t.addToParentLocked(newname, oe.data)
	return nil
}

// Stat returns a FileInfo describing the named file.
func (t *TxnMemMapFs) Stat(name string) (os.FileInfo, error) {
	name = normalizePath(name)
	t.mu.RLock()
	defer t.mu.RUnlock()
	e, err := t.openLocked(name)
	if err != nil {
		return nil, err
	}
	return mem.GetFileInfo(e.data), nil
}

// LstatIfPossible implements the optional Lstater interface.
func (t *TxnMemMapFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	fi, err := t.Stat(name)
	return fi, false, err
}

// Chmod changes the mode of the named file.
func (t *TxnMemMapFs) Chmod(name string, mode os.FileMode) error {
	mode &= chmodBits
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	e, err := t.openLocked(name)
	if err != nil {
		return &os.PathError{Op: "chmod", Path: name, Err: ErrFileNotFound}
	}
	fi := mem.GetFileInfo(e.data)
	other := fi.Mode() &^ os.FileMode(chmodBits)
	mem.SetMode(e.data, other|mode)
	t.bumpLocked(e)
	return nil
}

// Chown changes the uid/gid of the named file.
func (t *TxnMemMapFs) Chown(name string, uid, gid int) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	e, err := t.openLocked(name)
	if err != nil {
		return &os.PathError{Op: "chown", Path: name, Err: ErrFileNotFound}
	}
	mem.SetUID(e.data, uid)
	mem.SetGID(e.data, gid)
	t.bumpLocked(e)
	return nil
}

// Chtimes changes the access and modification times of the named file.
func (t *TxnMemMapFs) Chtimes(name string, atime, mtime time.Time) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	e, err := t.openLocked(name)
	if err != nil {
		return &os.PathError{Op: "chtimes", Path: name, Err: ErrFileNotFound}
	}
	mem.SetModTime(e.data, mtime)
	t.bumpLocked(e)
	return nil
}

func (t *TxnMemMapFs) newWritableHandleLocked(fd *mem.FileData) File {
	h := mem.NewFileHandle(fd)
	return &txnBaseFile{File: h, fs: t, fd: fd}
}

// txnBaseFile wraps a writable mem file handle so that content and metadata
// changes bump the entry's version, allowing concurrent transactions to
// observe them.
type txnBaseFile struct {
	File
	fs *TxnMemMapFs
	fd *mem.FileData
}

func (f *txnBaseFile) nameLocked() (string, bool) {
	n := mem.FullName(f.fd)
	if e, ok := f.fs.data[n]; ok && e.data == f.fd {
		return n, true
	}
	return "", false
}

func (f *txnBaseFile) bump() {
	f.fs.mu.Lock()
	if name, ok := f.nameLocked(); ok {
		f.fs.bumpLocked(f.fs.data[name])
	}
	f.fs.mu.Unlock()
}

func (f *txnBaseFile) Write(b []byte) (int, error) {
	n, err := f.File.Write(b)
	if n > 0 {
		f.bump()
	}
	return n, err
}

func (f *txnBaseFile) WriteAt(b []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(b, off)
	if n > 0 {
		f.bump()
	}
	return n, err
}

func (f *txnBaseFile) WriteString(s string) (int, error) {
	n, err := f.File.WriteString(s)
	if n > 0 {
		f.bump()
	}
	return n, err
}

func (f *txnBaseFile) Truncate(size int64) error {
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	f.bump()
	return nil
}

func (f *txnBaseFile) Close() error {
	if err := f.File.Close(); err != nil {
		return err
	}
	f.bump()
	return nil
}

// applyBatch validates and applies b atomically. If any recorded version no
// longer matches, nothing is changed and a *TxnConflictError is returned.
func (t *TxnMemMapFs) applyBatch(b *batch) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	data := t.getData()
	conflict := func(name string) error {
		return &TxnConflictError{Path: name}
	}

	check := func(name string, want uint64) error {
		cur, ok := data[name]
		var have uint64
		if ok {
			have = cur.version
		}
		if have != want {
			return conflict(name)
		}
		return nil
	}

	// Phase 1: verify everything under the write lock, before mutating.
	for p, v := range b.expect {
		if err := check(p, v); err != nil {
			return err
		}
	}
	for _, p := range b.puts {
		if p.resurrect {
			continue
		}
		if err := check(p.name, p.expectedVersion); err != nil {
			return err
		}
		// A file must never silently replace a directory or vice versa.
		cur, exists := data[p.name]
		putIsDir := mem.GetFileInfo(p.data).IsDir()
		if exists && cur.isDir() != putIsDir {
			return conflict(p.name)
		}
	}
	removed := make(map[string]bool)
	for _, r := range b.removes {
		if err := check(r.name, r.expectedVersion); err != nil {
			return err
		}
		// Every descendant observed at read time must still be there, with the
		// same version, and no new descendant may have appeared.
		live := t.liveDescendantsLocked(r.name)
		if len(live) != len(r.expectedDescendants) {
			return conflict(r.name)
		}
		for p, v := range r.expectedDescendants {
			if live[p] != v {
				return conflict(r.name)
			}
		}
		for p := range live {
			removed[p] = true
		}
		removed[r.name] = true
	}

	// Phase 2: apply the removals (whiteouts before puts, since a put of the
	// root of a removed tree resurrects that root).
	for _, r := range b.removes {
		// Detach the whiteout root from its parent (unless the parent itself
		// is removed).
		if parent := filepath.Dir(r.name); !removed[parent] {
			t.removeFromParentLocked(r.name)
		}
		delete(data, r.name)
		for p := range r.expectedDescendants {
			delete(data, p)
		}
	}

	// Phase 3a: apply file/replacement puts and create fresh directory
	// entries. Merge directories are only marked here; their parent listings
	// are reconciled in phase 3b, after every child put has reached the map,
	// so they always point at the live published nodes (not layer clones).
	sort.Slice(b.puts, func(i, j int) bool { return b.puts[i].name < b.puts[j].name })
	var mergeDirs []putOp
	for _, p := range b.puts {
		putIsDir := mem.GetFileInfo(p.data).IsDir()
		cur, exists := data[p.name]
		if exists && putIsDir && p.mergeDir {
			mergeDirs = append(mergeDirs, p)
			// The overlay may have metadata changes for the directory even
			// when merging; adopt them onto the live node.
			t.bumpLocked(cur)
			continue
		}
		if exists {
			t.removeFromParentLocked(p.name)
		}
		mem.ChangeFileName(p.data, p.name)
		e := &versionedEntry{data: p.data, version: t.allocVersionLocked()}
		data[p.name] = e
		t.addToParentLocked(p.name, p.data)
	}

	// Phase 3b: rebuild each merged directory's listing from the
	// authoritative path map. Direct children present in the map are exactly
	// the nodes that must appear: deletions removed their map entries,
	// resurrections/replacements inserted fresh ones, and untouched base
	// children are still there. Resolving pointers from the map guarantees
	// listings never reference deleted nodes or layer clones.
	for _, p := range mergeDirs {
		cur := data[p.name]
		wanted := make(map[string]*mem.FileData)
		for path, e := range data {
			if isDirectChildOf(p.name, path) {
				wanted[filepath.Base(path)] = e.data
			}
		}
		mem.ResetMemDir(cur.data)
		keys := make([]string, 0, len(wanted))
		for k := range wanted {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			mem.AddToMemDir(cur.data, wanted[k])
		}
	}

	return nil
}

// isDirectChildOf reports whether child is an immediate child of dir in the
// normalized rooted path space.
func isDirectChildOf(dir, child string) bool {
	if child == dir {
		return false
	}
	if dir == FilePathSeparator {
		return !strings.Contains(child[1:], FilePathSeparator)
	}
	rest := strings.TrimPrefix(child, dir+FilePathSeparator)
	return rest != "" && !strings.Contains(rest, FilePathSeparator) && child != rest
}

func (t *TxnMemMapFs) liveDescendantsLocked(root string) map[string]uint64 {
	out := make(map[string]uint64)
	if root == FilePathSeparator {
		for p, e := range t.getData() {
			if p != FilePathSeparator {
				out[p] = e.version
			}
		}
		return out
	}
	prefix := root + FilePathSeparator
	for p, e := range t.getData() {
		if strings.HasPrefix(p, prefix) {
			out[p] = e.version
		}
	}
	return out
}
