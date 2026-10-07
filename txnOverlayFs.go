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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/afero/mem"
)

var (
	_ Fs      = (*TxnOverlayFs)(nil)
	_ Lstater = (*TxnOverlayFs)(nil)
)

// ErrTxnClosed is returned when a transactional filesystem is used after it
// has been committed or rolled back.
var ErrTxnClosed = errors.New("afero: transaction already committed or rolled back")

// TxnConflictError reports that Commit() had to reject a transaction because a
// path the transaction read or modified changed in the base filesystem in the
// meantime. The whole transaction is left untouched (it must be rolled back
// explicitly).
type TxnConflictError struct {
	// Path is the first path whose recorded version no longer matched.
	Path string
}

func (e *TxnConflictError) Error() string {
	return fmt.Sprintf("afero: transaction conflict at %q", e.Path)
}

// whiteout marks a base path that is hidden inside a transaction. For a
// removed directory the entry also makes the recreated layer directory
// opaque, i.e. base children do not show through.
type whiteout struct {
	dir bool
}

// TxnOverlayFs is a transactional overlay filesystem.
//
// Reads return a merge of the read-write base filesystem with a private
// transaction layer; writes, removals and renames are only visible inside the
// transaction (and therefore never to anyone else looking at the base) until
// Commit() is called.
//
// The first time any path is read or rewritten, the current version of the
// base entry (and, for directory removals/renames, of every descendant) is
// recorded. Commit validates all recorded versions inside one critical
// section and applies every change in a single atomic batch, so outside
// observers either still see the old state or already see the complete new
// state. On a mismatch the commit is rejected as a whole with a
// *TxnConflictError and nothing is applied.
//
// After Commit() or Rollback() the transaction is closed and every further
// operation returns ErrTxnClosed.
type TxnOverlayFs struct {
	base *TxnMemMapFs

	mu      sync.Mutex
	closed  bool
	layer   *MemMapFs
	whites  map[string]whiteout
	readVer map[string]uint64
}

// NewTxnOverlayFs starts a new transaction over base. The returned filesystem
// implements Fs; changes stay private until Commit or are discarded with
// Rollback.
func NewTxnOverlayFs(base *TxnMemMapFs) *TxnOverlayFs {
	return &TxnOverlayFs{
		base:    base,
		layer:   NewMemMapFs().(*MemMapFs),
		whites:  make(map[string]whiteout),
		readVer: make(map[string]uint64),
	}
}

// Base returns the underlying base filesystem.
func (t *TxnOverlayFs) Base() *TxnMemMapFs { return t.base }

// Name returns the name of this filesystem.
func (*TxnOverlayFs) Name() string { return "TxnOverlayFs" }

func (t *TxnOverlayFs) checkOpenLocked() error {
	if t.closed {
		return ErrTxnClosed
	}
	return nil
}

// recordReadLocked snapshots the base version of name (and its parent) the
// first time the transaction touches them.
func (t *TxnOverlayFs) recordReadLocked(name string) {
	t.recordOneLocked(name)
	if parent := filepath.Dir(name); parent != name {
		t.recordOneLocked(parent)
	}
}

func (t *TxnOverlayFs) recordOneLocked(name string) {
	if _, ok := t.readVer[name]; ok {
		return
	}
	v, _ := t.base.EntryVersion(name)
	t.readVer[name] = v
}

// isWhiteLocked reports whether name is hidden by a whiteout (either exactly
// or because an ancestor directory was removed and recreated).
func (t *TxnOverlayFs) isWhiteLocked(name string) bool {
	if _, ok := t.whites[name]; ok {
		return true
	}
	// Opaque ancestor: a whiteout directory hides every base path below it.
	for p := range t.whites {
		w := t.whites[p]
		if !w.dir {
			continue
		}
		if p != name && strings.HasPrefix(name, p+FilePathSeparator) {
			return true
		}
	}
	return false
}

// baseAllowedLocked stats the base at name unless the merged view hides it.
func (t *TxnOverlayFs) baseAllowedLocked(name string) (os.FileInfo, bool) {
	if t.isWhiteLocked(name) {
		return nil, false
	}
	fi, err := t.base.Stat(name)
	if err != nil {
		return nil, false
	}
	return fi, true
}

// statLocked returns the merged FileInfo for name.
func (t *TxnOverlayFs) statLocked(name string) (os.FileInfo, error) {
	if fi, err := t.layer.Stat(name); err == nil {
		return fi, nil
	}
	if fi, ok := t.baseAllowedLocked(name); ok {
		return fi, nil
	}
	return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
}

// Stat returns a FileInfo describing the named file in the merged view.
func (t *TxnOverlayFs) Stat(name string) (os.FileInfo, error) {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return nil, err
	}
	t.recordReadLocked(name)
	return t.statLocked(name)
}

// LstatIfPossible implements the optional Lstater interface. The overlay has
// no symlinks of its own, so this is equivalent to Stat.
func (t *TxnOverlayFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	fi, err := t.Stat(name)
	return fi, false, err
}

// Open opens a file (or a merged view of a directory) for reading.
func (t *TxnOverlayFs) Open(name string) (File, error) {
	return t.OpenFile(name, os.O_RDONLY, 0)
}

// OpenFile opens a file using the given flags.
func (t *TxnOverlayFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return nil, err
	}
	t.recordReadLocked(name)

	writing := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0

	if !writing {
		// Layer first.
		if f, err := t.layer.Open(name); err == nil {
			fi, _ := f.Stat()
			if fi.IsDir() {
				// Present the merged (layer + base) directory listing.
				return t.openMergedDirLocked(name, f), nil
			}
			return &txnFile{File: f, txn: t}, nil
		}
		if bf, ok := t.baseAllowedLocked(name); ok {
			if bf.IsDir() {
				bf2, err := t.base.Open(name)
				if err != nil {
					return nil, err
				}
				return t.openMergedDirLocked(name, bf2), nil
			}
			return t.base.Open(name)
		}
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}

	// A mutating open must not silently turn a file ancestor into a
	// directory; mirror ENOTDIR from a real filesystem.
	if err := t.checkPathAncestorsLocked(name); err != nil {
		return nil, err
	}

	// Mutating open.
	if flag&os.O_EXCL != 0 {
		if _, err := t.statLocked(name); err == nil {
			return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileExists}
		}
	}

	exists := true
	if _, err := t.layer.Stat(name); err != nil {
		if bf, ok := t.baseAllowedLocked(name); ok {
			if bf.IsDir() {
				return nil, &os.PathError{Op: "open", Path: name, Err: syscallErrIsDir()}
			}
			if err := t.copyUpFileLocked(name); err != nil {
				return nil, err
			}
		} else {
			exists = false
			if flag&os.O_CREATE == 0 {
				return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
			}
		}
	}

	f, err := t.layer.OpenFile(name, flag, perm)
	if err != nil {
		// If the parent directory does not exist in either layer, behave
		// like a real filesystem.
		if !exists && t.missingParentLocked(name) {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
		}
		return nil, err
	}
	return &txnFile{File: f, txn: t}, nil
}

func syscallErrIsDir() error { return syscall.EISDIR }

func syscallENOTEMPTY() error { return syscall.ENOTEMPTY }

func syscallENOTDIR() error { return syscall.ENOTDIR }

func syscallEISDIR() error { return syscall.EISDIR }

func syscallEINVAL() error { return syscall.EINVAL }

func (t *TxnOverlayFs) missingParentLocked(name string) bool {
	dir := filepath.Dir(name)
	if dir == FilePathSeparator || dir == name {
		return false
	}
	if _, err := t.statLocked(dir); err != nil {
		return true
	}
	return false
}

// checkPathAncestorsLocked returns ENOTDIR if an ancestor of name exists (in
// the merged view) as a non-directory.
func (t *TxnOverlayFs) checkPathAncestorsLocked(name string) error {
	dir := filepath.Dir(name)
	if dir == FilePathSeparator || dir == name {
		return nil
	}
	if fi, err := t.statLocked(dir); err == nil && !fi.IsDir() {
		return &os.PathError{Op: "open", Path: name, Err: syscallENOTDIR()}
	}
	return nil
}

// copyUpFileLocked copies a base file (metadata + bytes) into the layer.
func (t *TxnOverlayFs) copyUpFileLocked(name string) error {
	fd, err := t.base.FileDataForCopy(name)
	if err != nil {
		return err
	}
	if err := t.ensureLayerParentLocked(name); err != nil {
		return err
	}
	h, err := t.layer.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	fi := mem.GetFileInfo(fd)
	_ = t.layer.Chmod(name, fi.Mode())
	src := mem.NewReadOnlyFileHandle(fd)
	if _, err := io.Copy(h, src); err != nil {
		_ = src.Close()
		_ = h.Close()
		return err
	}
	_ = src.Close()
	_ = t.layer.Chtimes(name, fi.ModTime(), fi.ModTime())
	return h.Close()
}

// copyUpSubtreeLocked materializes the whole merged subtree rooted at name
// into the layer. Used before renaming a directory so that base children
// travel with it.
func (t *TxnOverlayFs) copyUpSubtreeLocked(name string) error {
	fi, err := t.statLocked(name)
	if err != nil {
		return err
	}
	if _, err := t.layer.Stat(name); err != nil {
		if err := t.ensureLayerParentLocked(name); err != nil {
			return err
		}
		if fi.IsDir() {
			if err := t.layer.Mkdir(name, fi.Mode().Perm()); err != nil && !os.IsExist(err) {
				return err
			}
			_ = t.layer.Chtimes(name, fi.ModTime(), fi.ModTime())
		} else {
			if err := t.copyUpFileLocked(name); err != nil {
				return err
			}
		}
	}
	if !fi.IsDir() {
		return nil
	}
	children, err := t.mergedChildNamesLocked(name)
	if err != nil {
		return err
	}
	for _, c := range children {
		if err := t.copyUpSubtreeLocked(filepath.Join(name, c)); err != nil {
			return err
		}
	}
	return nil
}

func (t *TxnOverlayFs) ensureLayerParentLocked(name string) error {
	dir := filepath.Dir(name)
	if dir == FilePathSeparator {
		return nil
	}
	return t.layer.MkdirAll(dir, 0o777)
}

// Create creates or truncates a file in the transaction layer.
func (t *TxnOverlayFs) Create(name string) (File, error) {
	return t.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o666)
}

// Mkdir creates a directory in the transaction layer.
func (t *TxnOverlayFs) Mkdir(name string, perm os.FileMode) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	t.recordReadLocked(name)
	if _, err := t.statLocked(name); err == nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: ErrFileExists}
	}
	if err := t.checkPathAncestorsLocked(name); err != nil {
		return err
	}
	t.recordOneLocked(filepath.Dir(name))
	if err := t.layer.Mkdir(name, perm); err != nil {
		return err
	}
	return nil
}

// MkdirAll creates a directory and all missing parents in the transaction
// layer.
func (t *TxnOverlayFs) MkdirAll(name string, perm os.FileMode) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	t.recordReadLocked(name)
	if fi, err := t.statLocked(name); err == nil {
		if fi.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: name, Err: ErrFileExists}
	}
	if err := t.layer.MkdirAll(name, perm); err != nil {
		return err
	}
	return nil
}

// addWhiteLocked records a whiteout and snapshots the removed base subtree
// versions as visible through the merged view at removal time.
func (t *TxnOverlayFs) addWhiteLocked(name string, dir bool) {
	t.recordOneLocked(name)
	t.recordOneLocked(filepath.Dir(name))
	if dir {
		// Snapshot every base descendant currently visible through the merged
		// subtree. Layer-only descendants do not constrain the base.
		prefix := name + FilePathSeparator
		for _, p := range t.mergedSubtreePathsLocked(name) {
			if !strings.HasPrefix(p, prefix) {
				continue
			}
			t.recordOneLocked(p)
		}
	}
	t.whites[name] = whiteout{dir: dir}
}

// mergedSubtreePathsLocked returns the names of the node itself and all nodes
// visible through the merged view beneath it.
func (t *TxnOverlayFs) mergedSubtreePathsLocked(root string) []string {
	var out []string
	var walk func(dir string)
	walk = func(dir string) {
		out = append(out, dir)
		names, err := t.mergedChildNamesLocked(dir)
		if err != nil {
			return
		}
		for _, n := range names {
			full := filepath.Join(dir, n)
			fi, err := t.statLocked(full)
			if err != nil {
				continue
			}
			if fi.IsDir() {
				walk(full)
			} else {
				out = append(out, full)
			}
		}
	}
	walk(root)
	return out
}

// Remove removes a file or empty directory. The removal is a whiteout until
// commit.
func (t *TxnOverlayFs) Remove(name string) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	t.recordReadLocked(name)
	fi, err := t.statLocked(name)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	if fi.IsDir() {
		names, err := t.mergedChildNamesLocked(name)
		if err != nil {
			return err
		}
		if len(names) > 0 {
			return &os.PathError{Op: "remove", Path: name, Err: syscallENOTEMPTY()}
		}
	}
	if err := t.layer.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	t.addWhiteLocked(name, fi.IsDir())
	return nil
}

// RemoveAll removes a path and all children; the removal is a whiteout until
// commit.
func (t *TxnOverlayFs) RemoveAll(name string) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	t.recordReadLocked(name)
	fi, err := t.statLocked(name)
	if err != nil {
		return nil // RemoveAll never fails for a missing path.
	}
	t.purgeLayerSubtreeLocked(name)
	t.addWhiteLocked(name, fi.IsDir())
	return nil
}

func (t *TxnOverlayFs) purgeLayerSubtreeLocked(name string) {
	_ = t.layer.RemoveAll(name)
	// Remove layer descendants explicitly (MemMapFs.RemoveAll handles this,
	// but keep the guarantee explicit).
}

// Rename renames oldname to newname. The rename is private to the
// transaction; cross-directory renames update both parents' metadata as part
// of the eventual atomic commit.
func (t *TxnOverlayFs) Rename(oldname, newname string) error {
	oldname = normalizePath(oldname)
	newname = normalizePath(newname)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	if oldname == newname {
		t.recordReadLocked(oldname)
		return nil
	}
	// A path cannot be moved into one of its own descendants.
	if strings.HasPrefix(newname, oldname+FilePathSeparator) {
		return &os.PathError{Op: "rename", Path: newname, Err: syscallEINVAL()}
	}

	t.recordReadLocked(oldname)
	t.recordReadLocked(newname)
	src, err := t.statLocked(oldname)
	if err != nil {
		return &os.PathError{Op: "rename", Path: oldname, Err: os.ErrNotExist}
	}
	// Destination parent must be a directory.
	if parent := filepath.Dir(newname); parent != FilePathSeparator {
		if pfi, perr := t.statLocked(parent); perr == nil && !pfi.IsDir() {
			return &os.PathError{Op: "rename", Path: newname, Err: syscallENOTDIR()}
		}
	}
	dst, dstErr := t.statLocked(newname)
	if dstErr == nil && dst.IsDir() {
		if !src.IsDir() {
			return &os.PathError{Op: "rename", Path: newname, Err: syscallEISDIR()}
		}
		names, err := t.mergedChildNamesLocked(newname)
		if err != nil {
			return err
		}
		if len(names) > 0 {
			return &os.PathError{Op: "rename", Path: newname, Err: syscallENOTEMPTY()}
		}
	}

	// Materialize the whole merged source subtree so that base children
	// travel with the rename (this is what makes rename loops work).
	if err := t.copyUpSubtreeLocked(oldname); err != nil {
		return err
	}

	// Remove a merged destination (file or empty merged directory).
	if dstErr == nil {
		t.purgeLayerSubtreeLocked(newname)
		if bf, ok := t.baseAllowedLocked(newname); ok {
			t.addWhiteLocked(newname, bf.IsDir())
		}
	}

	// Whiteout the old name so the base entry (and, for directories, any
	// base descendants that were copied up and moved along) does not show
	// through at the source path. For a file the base node itself must be
	// hidden; for a directory, a whiteout is also needed when the base had
	// descendants even if the root node is handled otherwise.
	oldHasBase := false
	if _, err := t.base.Stat(oldname); err == nil {
		oldHasBase = true
	}
	if src.IsDir() && t.baseHasDescendantLocked(oldname) {
		oldHasBase = true
	}
	if oldHasBase {
		t.addWhiteLocked(oldname, src.IsDir())
	}

	if err := t.ensureLayerParentLocked(newname); err != nil {
		return err
	}
	if err := t.layer.Rename(oldname, newname); err != nil {
		return err
	}
	// The source's whiteout is now harmless if the layer left nothing there;
	// it correctly hides the base. Descendant layer nodes moved along.
	return nil
}

func (t *TxnOverlayFs) baseHasDescendantLocked(name string) bool {
	return len(t.base.DescendantVersions(name)) > 0
}

// Chmod changes the mode of the named file, copying it up if needed.
func (t *TxnOverlayFs) Chmod(name string, mode os.FileMode) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	t.recordReadLocked(name)
	if _, err := t.statLocked(name); err != nil {
		return &os.PathError{Op: "chmod", Path: name, Err: os.ErrNotExist}
	}
	if err := t.copyUpIfNeededLocked(name); err != nil {
		return err
	}
	return t.layer.Chmod(name, mode)
}

// Chown changes the uid/gid of the named file, copying it up if needed.
func (t *TxnOverlayFs) Chown(name string, uid, gid int) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	t.recordReadLocked(name)
	if _, err := t.statLocked(name); err != nil {
		return &os.PathError{Op: "chown", Path: name, Err: os.ErrNotExist}
	}
	if err := t.copyUpIfNeededLocked(name); err != nil {
		return err
	}
	return t.layer.Chown(name, uid, gid)
}

// Chtimes changes the times of the named file, copying it up if needed.
func (t *TxnOverlayFs) Chtimes(name string, atime, mtime time.Time) error {
	name = normalizePath(name)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	t.recordReadLocked(name)
	if _, err := t.statLocked(name); err != nil {
		return &os.PathError{Op: "chtimes", Path: name, Err: os.ErrNotExist}
	}
	if err := t.copyUpIfNeededLocked(name); err != nil {
		return err
	}
	return t.layer.Chtimes(name, atime, mtime)
}

func (t *TxnOverlayFs) copyUpIfNeededLocked(name string) error {
	if _, err := t.layer.Stat(name); err == nil {
		return nil
	}
	fi, ok := t.baseAllowedLocked(name)
	if !ok {
		return &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}
	if err := t.ensureLayerParentLocked(name); err != nil {
		return err
	}
	if fi.IsDir() {
		if err := t.layer.Mkdir(name, fi.Mode().Perm()); err != nil && !os.IsExist(err) {
			return err
		}
		return t.layer.Chtimes(name, fi.ModTime(), fi.ModTime())
	}
	return t.copyUpFileLocked(name)
}

// Commit publishes all transactional changes to the base in one atomic batch.
// It returns a *TxnConflictError without applying anything if a recorded base
// version has changed. A second Commit (or any Commit after Rollback) returns
// ErrTxnClosed.
func (t *TxnOverlayFs) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}

	b, err := t.buildBatchLocked()
	if err != nil {
		return err
	}
	if err := t.base.applyBatch(b); err != nil {
		return err
	}
	t.closed = true
	return nil
}

// Rollback discards all transactional changes and closes the transaction.
func (t *TxnOverlayFs) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	t.closed = true
	t.layer = nil
	t.whites = nil
	t.readVer = nil
	return nil
}

func (t *TxnOverlayFs) buildBatchLocked() (*batch, error) {
	b := &batch{expect: make(map[string]uint64)}
	for p, v := range t.readVer {
		b.expect[p] = v
	}

	// Materialize every whiteout into a removeOp. The recorded (read-time)
	// versions come from t.readVer; a base change between read time and now
	// is rejected below.
	whiteNames := make([]string, 0, len(t.whites))
	for p := range t.whites {
		whiteNames = append(whiteNames, p)
	}
	sort.Strings(whiteNames)
	nestedInAnotherWhiteout := func(self, p string) bool {
		for _, w := range whiteNames {
			if w == self {
				continue
			}
			if strings.HasPrefix(p, w+FilePathSeparator) {
				return true
			}
		}
		return false
	}
	for _, name := range whiteNames {
		w := t.whites[name]
		if _, exists := t.base.EntryVersion(name); !exists {
			// Nothing in the base to remove.
			continue
		}
		descs := make(map[string]uint64)
		if w.dir {
			// Descendants recorded at removal time. Only paths that exist in
			// the base participate: layer-only nodes are published as puts
			// and never constrain the removal.
			recorded := make(map[string]uint64)
			prefix := name + FilePathSeparator
			for p, v := range t.readVer {
				if !strings.HasPrefix(p, prefix) {
					continue
				}
				if _, exists := t.base.EntryVersion(p); exists {
					recorded[p] = v
				}
			}
			// Descendants currently in the base. Any difference between the
			// two sets is a conflict detected by applyBatch.
			live := t.base.DescendantVersions(name)
			for p := range live {
				if nestedInAnotherWhiteout(name, p) {
					continue
				}
				if rv, ok := recorded[p]; ok {
					descs[p] = rv
				} else {
					descs[p] = 0 // unknown -> guaranteed mismatch
				}
			}
			for p, rv := range recorded {
				if nestedInAnotherWhiteout(name, p) {
					continue
				}
				if _, ok := live[p]; !ok {
					descs[p] = rv // vanished -> mismatch (live has 0)
				}
			}
		}
		b.removes = append(b.removes, removeOp{
			name:                name,
			expectedVersion:     t.readVer[name],
			expectedDescendants: descs,
		})
	}

	// Enumerate the entire layer tree.
	layerNodes, err := enumerateLayerLocked(t.layer)
	if err != nil {
		return nil, err
	}
	for _, node := range layerNodes {
		name := node.path
		exp, baseExists := t.base.EntryVersion(name)
		fi := mem.GetFileInfo(node.data)
		_, isWhite := t.whites[name]
		// The node can only be merged into a live base directory if no
		// whiteout deletes it directly or one of its ancestors in this same
		// batch. A directory hidden by an ancestor whiteout is (re)created
		// fresh, not merged.
		merge := fi.IsDir() && !isWhite && baseExists && !t.underWhiteoutLocked(name)
		if isWhite || t.underWhiteoutLocked(name) {
			// The matching removeOp deletes any pre-existing base node first;
			// this put recreates a fresh node afterwards.
			exp = 0
		}
		b.puts = append(b.puts, putOp{
			name:            name,
			data:            cloneFileData(node.data),
			expectedVersion: exp,
			mergeDir:        merge,
			resurrect:       isWhite || t.underWhiteoutLocked(name),
		})
	}
	return b, nil
}

// underWhiteoutLocked reports whether name is strictly inside a directory
// whiteout (i.e. an ancestor of name carries a whiteout).
func (t *TxnOverlayFs) underWhiteoutLocked(name string) bool {
	for p, w := range t.whites {
		if w.dir && p != name && strings.HasPrefix(name, p+FilePathSeparator) {
			return true
		}
	}
	return false
}

type layerNode struct {
	path string
	data *mem.FileData
}

// enumerateLayerLocked returns every node stored in the private layer in
// deterministic (path) order, including files and directories.
func enumerateLayerLocked(l *MemMapFs) ([]layerNode, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	paths := make([]string, 0, len(l.data))
	for p := range l.getData() {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	out := make([]layerNode, 0, len(paths))
	for _, p := range paths {
		out = append(out, layerNode{path: p, data: l.data[p]})
	}
	return out, nil
}

// mergedChildNamesLocked returns the sorted base names of a directory as
// visible through the merged view.
func (t *TxnOverlayFs) mergedChildNamesLocked(name string) ([]string, error) {
	seen := make(map[string]bool)
	var out []string
	add := func(names []string) {
		for _, n := range names {
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	if f, err := t.layer.Open(name); err == nil {
		fi, _ := f.Stat()
		if fi.IsDir() {
			names, err := f.Readdirnames(-1)
			_ = f.Close()
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, err
			}
			add(names)
		} else {
			_ = f.Close()
			return nil, &os.PathError{Op: "readdirent", Path: name, Err: syscallENOTDIR()}
		}
	}
	if _, opaque := t.whites[name]; !opaque {
		if bf, ok := t.baseAllowedLocked(name); ok && bf.IsDir() {
			if f, err := t.base.Open(name); err == nil {
				names, err := f.Readdirnames(-1)
				_ = f.Close()
				if err != nil && !errors.Is(err, io.EOF) {
					return nil, err
				}
				// Hide exact whiteout children.
				var filtered []string
				for _, n := range names {
					full := filepath.Join(name, n)
					if t.isWhiteLocked(full) {
						continue
					}
					filtered = append(filtered, n)
				}
				add(filtered)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// openMergedDirLocked wraps a directory name with a synthetic merged directory
// listing.
func (t *TxnOverlayFs) openMergedDirLocked(name string, _ File) File {
	return &txnDirFile{name: name, txn: t}
}

// txnFile wraps a layer file handle so it cannot be used after the
// transaction has been closed.
type txnFile struct {
	File
	txn *TxnOverlayFs
}

var errFileHandleClosed = errors.New("afero: transaction file handle is closed")

func (f *txnFile) check() error {
	f.txn.mu.Lock()
	defer f.txn.mu.Unlock()
	if f.txn.closed {
		return errFileHandleClosed
	}
	return nil
}

func (f *txnFile) Write(b []byte) (int, error) {
	if err := f.check(); err != nil {
		return 0, err
	}
	return f.File.Write(b)
}

func (f *txnFile) WriteAt(b []byte, off int64) (int, error) {
	if err := f.check(); err != nil {
		return 0, err
	}
	return f.File.WriteAt(b, off)
}

func (f *txnFile) WriteString(s string) (int, error) {
	if err := f.check(); err != nil {
		return 0, err
	}
	return f.File.WriteString(s)
}

func (f *txnFile) Truncate(size int64) error {
	if err := f.check(); err != nil {
		return err
	}
	return f.File.Truncate(size)
}

func (f *txnFile) Close() error { return f.File.Close() }

// txnDirFile is a read-only directory handle presenting the merged listing.
type txnDirFile struct {
	name   string
	txn    *TxnOverlayFs
	buf    []os.FileInfo
	at     int
	closed bool
}

func (d *txnDirFile) build() ([]os.FileInfo, error) {
	d.txn.mu.Lock()
	defer d.txn.mu.Unlock()
	if d.txn.closed {
		return nil, ErrTxnClosed
	}
	names, err := d.txn.mergedChildNamesLocked(d.name)
	if err != nil {
		return nil, err
	}
	out := make([]os.FileInfo, 0, len(names))
	for _, n := range names {
		fi, err := d.txn.statLocked(filepath.Join(d.name, n))
		if err != nil {
			continue
		}
		out = append(out, fi)
	}
	return out, nil
}

func (d *txnDirFile) Close() error {
	if d.closed {
		return ErrFileClosed
	}
	d.closed = true
	d.buf = nil
	return nil
}

func (d *txnDirFile) Read(_ []byte) (int, error) {
	return 0, &os.PathError{Op: "read", Path: d.name, Err: errors.New("is a directory")}
}

func (d *txnDirFile) ReadAt(_ []byte, _ int64) (int, error) {
	return 0, &os.PathError{Op: "read", Path: d.name, Err: errors.New("is a directory")}
}

func (d *txnDirFile) Write(_ []byte) (int, error) {
	return 0, &os.PathError{Op: "write", Path: d.name, Err: errors.New("is a directory")}
}

func (d *txnDirFile) WriteAt(_ []byte, _ int64) (int, error) {
	return 0, &os.PathError{Op: "write", Path: d.name, Err: errors.New("is a directory")}
}

func (d *txnDirFile) WriteString(_ string) (int, error) { return d.Write(nil) }

func (d *txnDirFile) Truncate(_ int64) error {
	return &os.PathError{Op: "truncate", Path: d.name, Err: errors.New("is a directory")}
}

func (d *txnDirFile) Sync() error { return nil }

func (d *txnDirFile) Name() string { return d.name }

func (d *txnDirFile) Stat() (os.FileInfo, error) {
	d.txn.mu.Lock()
	defer d.txn.mu.Unlock()
	if d.txn.closed {
		return nil, ErrTxnClosed
	}
	return d.txn.statLocked(d.name)
}

func (d *txnDirFile) Seek(_ int64, _ int) (int64, error) { return 0, nil }

func (d *txnDirFile) Readdir(count int) ([]os.FileInfo, error) {
	if d.closed {
		return nil, ErrFileClosed
	}
	if d.buf == nil {
		fis, err := d.build()
		if err != nil {
			return nil, err
		}
		d.buf = fis
	}
	if count <= 0 {
		res := d.buf[d.at:]
		d.at = len(d.buf)
		if len(res) == 0 {
			return res, io.EOF
		}
		return res, nil
	}
	if d.at >= len(d.buf) {
		return nil, io.EOF
	}
	end := d.at + count
	if end > len(d.buf) {
		end = len(d.buf)
	}
	res := d.buf[d.at:end]
	d.at = end
	if d.at >= len(d.buf) {
		return res, io.EOF
	}
	return res, nil
}

func (d *txnDirFile) Readdirnames(n int) ([]string, error) {
	fis, err := d.Readdir(n)
	names := make([]string, len(fis))
	for i, fi := range fis {
		names[i] = fi.Name()
	}
	return names, err
}
