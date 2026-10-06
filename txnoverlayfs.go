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
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/afero/internal/common"
)

var (
	_ Fs               = (*TxnOverlayFs)(nil)
	_ Lstater          = (*TxnOverlayFs)(nil)
	_ iofs.ReadDirFile = (*txnDirFile)(nil)
)

// TxnOverlayFs is a transactional overlay filesystem. Reads merge the base
// filesystem with a private in-memory transaction layer, while all writes,
// removals and renames are only staged in the transaction layer and remain
// invisible to the base. An explicit Commit publishes the staged changes to
// a base that supports atomic batches (TxnCommitter, e.g. MemMapFs).
//
// The transaction records the initial base version of every path it reads
// or modifies. At commit time, if any of those paths changed in the base
// underneath the transaction, the whole commit is rejected with a
// *TxnConflictError and the base is left untouched. Removals of base paths
// (including whole directories) are tracked with whiteout markers, and
// renames record the source and target subtrees together with the metadata
// (versions) of both parent directories.
type TxnOverlayFs struct {
	base Fs

	mu            sync.Mutex
	layer         Fs
	versions      map[string]uint64
	whiteouts     map[string]bool // normalized path -> isDir
	renameSources map[string]bool
	renameTargets map[string]bool
	renames       []TxnOp
	metaOps       []TxnOp
	committed     bool
}

// NewTxnOverlayFs creates a new transactional overlay on top of base. To be
// able to commit, base must implement TxnCommitter (MemMapFs does).
func NewTxnOverlayFs(base Fs) *TxnOverlayFs {
	t := &TxnOverlayFs{base: base}
	t.resetLocked()
	return t
}

func (t *TxnOverlayFs) resetLocked() {
	t.layer = NewMemMapFs()
	t.versions = make(map[string]uint64)
	t.whiteouts = make(map[string]bool)
	t.renameSources = make(map[string]bool)
	t.renameTargets = make(map[string]bool)
	t.renames = nil
	t.metaOps = nil
}

// Name returns the name of this filesystem.
func (t *TxnOverlayFs) Name() string { return "TxnOverlayFs" }

// record stores the current base version of name the first time the
// transaction touches it.
func (t *TxnOverlayFs) record(name string) {
	var v uint64
	if vf, ok := t.base.(versionedFs); ok {
		v = vf.PathVersion(name)
	}
	name = normalizePath(name)
	t.mu.Lock()
	if _, ok := t.versions[name]; !ok {
		t.versions[name] = v
	}
	t.mu.Unlock()
}

// recordTree records the versions of root and, if root is a directory in
// the base, of its whole subtree.
func (t *TxnOverlayFs) recordTree(root string) {
	t.record(root)
	fi, err := t.base.Stat(root)
	if err != nil || !fi.IsDir() {
		return
	}
	_ = Walk(t.base, root, func(p string, info os.FileInfo, err error) error {
		if err == nil {
			t.record(p)
		}
		return nil
	})
}

// whitedOutLocked reports whether name or any of its ancestors carries a
// whiteout marker, i.e. whether the base view of name is hidden. The caller
// must hold t.mu.
func (t *TxnOverlayFs) whitedOutLocked(name string) bool {
	name = normalizePath(name)
	for {
		if _, ok := t.whiteouts[name]; ok {
			return true
		}
		parent := parentDir(name)
		if parent == name {
			return false
		}
		name = parent
	}
}

func (t *TxnOverlayFs) isWhitedOut(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.whitedOutLocked(name)
}

func (t *TxnOverlayFs) checkActive() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return ErrTxnCommitted
	}
	return nil
}

// copyUp copies a single base file (or directory, without children) into
// the transaction layer, preserving content, permissions and modtime.
func (t *TxnOverlayFs) copyUp(name string) error {
	bfi, err := t.base.Stat(name)
	if err != nil {
		return err
	}
	if bfi.IsDir() {
		return t.layer.MkdirAll(name, bfi.Mode().Perm())
	}
	data, err := ReadFile(t.base, name)
	if err != nil {
		return err
	}
	if err := t.layer.MkdirAll(parentDir(name), 0o755); err != nil {
		return err
	}
	if err := WriteFile(t.layer, name, data, bfi.Mode().Perm()); err != nil {
		return err
	}
	return t.layer.Chtimes(name, bfi.ModTime(), bfi.ModTime())
}

// copySubtreeUp copies the whole base subtree rooted at oldname into the
// layer at newname. Entries already present in the layer win.
func copySubtreeUp(base, layer Fs, oldname, newname string) error {
	return Walk(base, oldname, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(oldname, p)
		if err != nil {
			return err
		}
		target := newname
		if rel != "." {
			target = filepath.Join(newname, rel)
		}
		if fi.IsDir() {
			if _, err := layer.Stat(target); err == nil {
				return nil
			}
			return layer.MkdirAll(target, fi.Mode().Perm())
		}
		if _, err := layer.Stat(target); err == nil {
			return nil
		}
		data, err := ReadFile(base, p)
		if err != nil {
			return err
		}
		if err := WriteFile(layer, target, data, fi.Mode().Perm()); err != nil {
			return err
		}
		return layer.Chtimes(target, fi.ModTime(), fi.ModTime())
	})
}

// Stat returns the merged view of name: the transaction layer wins over
// whiteouts, whiteouts win over the base.
func (t *TxnOverlayFs) Stat(name string) (os.FileInfo, error) {
	t.record(name)
	if fi, err := t.layer.Stat(name); err == nil {
		return fi, nil
	}
	if t.isWhitedOut(name) {
		return nil, &os.PathError{Op: "stat", Path: name, Err: ErrFileNotFound}
	}
	return t.base.Stat(name)
}

// LstatIfPossible implements Lstater. TxnOverlayFs does not support
// symlinks, so this behaves like Stat.
func (t *TxnOverlayFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	fi, err := t.Stat(name)
	return fi, false, err
}

// Open opens name for reading, merging base and transaction layer.
// Directories are returned as a merged snapshot of both layers.
func (t *TxnOverlayFs) Open(name string) (File, error) {
	t.record(name)
	fi, err := t.Stat(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
	}
	if fi.IsDir() {
		return t.openDir(name, fi)
	}
	if f, err := t.layer.Open(name); err == nil {
		return f, nil
	}
	if t.isWhitedOut(name) {
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
	}
	return t.base.Open(name)
}

// openDir returns a directory handle over the merged listing of base and
// layer, with whited-out base entries hidden.
func (t *TxnOverlayFs) openDir(name string, fi os.FileInfo) (File, error) {
	merged := make(map[string]os.FileInfo)

	if !t.isWhitedOut(name) {
		if bf, err := t.base.Open(name); err == nil {
			names, _ := bf.Readdirnames(-1)
			_ = bf.Close()
			for _, n := range names {
				child := filepath.Join(name, n)
				if t.isWhitedOut(child) {
					continue
				}
				if cfi, err := t.layer.Stat(child); err == nil {
					merged[n] = cfi
				} else if cfi, err := t.base.Stat(child); err == nil {
					merged[n] = cfi
				}
			}
		}
	}

	if lf, err := t.layer.Open(name); err == nil {
		names, _ := lf.Readdirnames(-1)
		_ = lf.Close()
		for _, n := range names {
			child := filepath.Join(name, n)
			if cfi, err := t.layer.Stat(child); err == nil {
				merged[n] = cfi
			}
		}
	}

	entries := make([]os.FileInfo, 0, len(merged))
	for _, cfi := range merged {
		entries = append(entries, cfi)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	return &txnDirFile{name: normalizePath(name), fi: fi, entries: entries}, nil
}

// OpenFile opens name with the given flags. Any write-ish flag stages the
// operation in the transaction layer (copying the file up from the base
// first if necessary); the base is never modified.
func (t *TxnOverlayFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) == 0 {
		return t.openReadOnly(name, flag, perm)
	}

	if err := t.checkActive(); err != nil {
		return nil, err
	}
	t.record(name)
	t.record(parentDir(name))

	_, layerErr := t.layer.Stat(name)
	inLayer := layerErr == nil
	_, baseErr := t.base.Stat(name)
	whitedOut := t.isWhitedOut(name)
	// The merged view contains name if the layer has it, or if the base
	// has it and no whiteout (on it or an ancestor) hides it.
	exists := inLayer || (baseErr == nil && !whitedOut)

	switch {
	case !exists:
		if flag&os.O_CREATE == 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
		}
		// The parent directory must exist in the merged view; a parent
		// that only exists in the layer (e.g. recreated after a removal)
		// is fine.
		pfi, perr := t.Stat(parentDir(name))
		if perr != nil || !pfi.IsDir() {
			return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
		}
		if err := t.layer.MkdirAll(parentDir(name), 0o755); err != nil {
			return nil, err
		}
	case !inLayer:
		// Exists in the base only: copy up before modifying.
		if flag&os.O_CREATE > 0 && flag&os.O_EXCL > 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileExists}
		}
		if err := t.copyUp(name); err != nil {
			return nil, err
		}
	}
	return t.layer.OpenFile(name, flag, perm)
}

func (t *TxnOverlayFs) openReadOnly(name string, flag int, perm os.FileMode) (File, error) {
	t.record(name)
	fi, err := t.Stat(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
	}
	if fi.IsDir() {
		return t.openDir(name, fi)
	}
	if f, err := t.layer.OpenFile(name, flag, perm); err == nil {
		return f, nil
	}
	if t.isWhitedOut(name) {
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
	}
	return t.base.OpenFile(name, flag, perm)
}

// Create creates a file in the transaction layer.
func (t *TxnOverlayFs) Create(name string) (File, error) {
	return t.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o666)
}

// Mkdir creates a directory in the transaction layer.
func (t *TxnOverlayFs) Mkdir(name string, perm os.FileMode) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	t.record(name)
	t.record(parentDir(name))
	if _, err := t.Stat(name); err == nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: ErrFileExists}
	}
	if _, err := t.Stat(parentDir(name)); err != nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: ErrFileNotFound}
	}
	return t.layer.MkdirAll(name, perm)
}

// MkdirAll creates a directory path in the transaction layer.
func (t *TxnOverlayFs) MkdirAll(path string, perm os.FileMode) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	t.record(path)
	t.record(parentDir(path))
	if fi, err := t.Stat(path); err == nil {
		if fi.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: ErrFileExists}
	}
	return t.layer.MkdirAll(path, perm)
}

// Remove removes name from the merged view. Base paths are hidden with a
// whiteout marker; the base itself is not modified.
func (t *TxnOverlayFs) Remove(name string) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	t.record(name)
	t.record(parentDir(name))

	fi, err := t.Stat(name)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: ErrFileNotFound}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if fi.IsDir() {
		_ = t.layer.RemoveAll(name)
	} else {
		_ = t.layer.Remove(name)
	}
	if _, err := t.base.Stat(name); err == nil {
		t.whiteouts[normalizePath(name)] = fi.IsDir()
	}
	return nil
}

// RemoveAll removes path and any children from the merged view. A base
// directory is hidden with a single whiteout marker on its root.
func (t *TxnOverlayFs) RemoveAll(path string) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	t.recordTree(path)
	t.record(parentDir(path))

	t.mu.Lock()
	defer t.mu.Unlock()
	_ = t.layer.RemoveAll(path)
	if fi, err := t.base.Stat(path); err == nil {
		t.whiteouts[normalizePath(path)] = fi.IsDir()
	}
	return nil
}

// Rename renames oldname to newname within the transaction. Base content is
// copied up into the layer and the base source is hidden with a whiteout;
// the base itself is only renamed on Commit. The versions of both parent
// directories are recorded so that concurrent structural changes to either
// parent are detected as conflicts.
func (t *TxnOverlayFs) Rename(oldname, newname string) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	if normalizePath(oldname) == normalizePath(newname) {
		return nil
	}

	// Record the initial versions of everything this rename touches:
	// both subtrees and both parent directories.
	t.recordTree(oldname)
	t.recordTree(newname)
	t.record(parentDir(oldname))
	t.record(parentDir(newname))

	pfi, perr := t.Stat(parentDir(newname))
	if perr != nil || !pfi.IsDir() {
		return &os.PathError{Op: "rename", Path: newname, Err: ErrFileNotFound}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	oldNorm := normalizePath(oldname)
	newNorm := normalizePath(newname)

	var oldLayerFi, oldBaseFi os.FileInfo
	oldLayerFi, _ = t.layer.Stat(oldname)
	if !t.whitedOutLocked(oldname) {
		oldBaseFi, _ = t.base.Stat(oldname)
	}
	if oldLayerFi == nil && oldBaseFi == nil {
		return &os.PathError{Op: "rename", Path: oldname, Err: ErrFileNotFound}
	}

	// If the destination exists in the base it is replaced: hide it.
	if fi, err := t.base.Stat(newname); err == nil {
		t.whiteouts[newNorm] = fi.IsDir()
	}
	// Clear any previously staged content at the destination.
	_ = t.layer.RemoveAll(newname)
	if err := t.layer.MkdirAll(parentDir(newname), 0o755); err != nil {
		return err
	}

	// Move staged (layer) content, then copy up base content the layer
	// does not already cover.
	if oldLayerFi != nil {
		if err := t.layer.Rename(oldname, newname); err != nil {
			return err
		}
	}
	if oldBaseFi != nil {
		if err := copySubtreeUp(t.base, t.layer, oldname, newname); err != nil {
			return err
		}
		t.whiteouts[oldNorm] = oldBaseFi.IsDir()
	}

	// The base needs a rename if it currently has oldname, or if an
	// earlier staged rename will place content at oldname during commit.
	if oldBaseFi != nil || t.renameTargets[oldNorm] {
		t.renames = append(t.renames, TxnOp{Kind: TxnOpRename, Path: oldname, Target: newname})
	}
	t.renameSources[oldNorm] = true
	t.renameTargets[newNorm] = true
	delete(t.renameTargets, oldNorm)
	return nil
}

// Chmod changes the mode of name within the transaction.
func (t *TxnOverlayFs) Chmod(name string, mode os.FileMode) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	if err := t.stageForMeta(name, "chmod"); err != nil {
		return err
	}
	if err := t.layer.Chmod(name, mode); err != nil {
		return err
	}
	t.mu.Lock()
	t.metaOps = append(t.metaOps, TxnOp{Kind: TxnOpChmod, Path: name, Perm: mode})
	t.mu.Unlock()
	return nil
}

// Chtimes changes the times of name within the transaction.
func (t *TxnOverlayFs) Chtimes(name string, atime, mtime time.Time) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	if err := t.stageForMeta(name, "chtimes"); err != nil {
		return err
	}
	if err := t.layer.Chtimes(name, atime, mtime); err != nil {
		return err
	}
	t.mu.Lock()
	t.metaOps = append(t.metaOps, TxnOp{Kind: TxnOpChtimes, Path: name, Atime: atime, Mtime: mtime})
	t.mu.Unlock()
	return nil
}

// Chown changes the ownership of name within the transaction.
func (t *TxnOverlayFs) Chown(name string, uid, gid int) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	if err := t.stageForMeta(name, "chown"); err != nil {
		return err
	}
	if err := t.layer.Chown(name, uid, gid); err != nil {
		return err
	}
	t.mu.Lock()
	t.metaOps = append(t.metaOps, TxnOp{Kind: TxnOpChown, Path: name, Uid: uid, Gid: gid})
	t.mu.Unlock()
	return nil
}

// stageForMeta makes sure name is present in the layer so metadata
// operations can be staged on it.
func (t *TxnOverlayFs) stageForMeta(name, op string) error {
	t.record(name)
	if _, err := t.layer.Stat(name); err == nil {
		return nil
	}
	if t.isWhitedOut(name) {
		return &os.PathError{Op: op, Path: name, Err: ErrFileNotFound}
	}
	if _, err := t.base.Stat(name); err != nil {
		return &os.PathError{Op: op, Path: name, Err: ErrFileNotFound}
	}
	return t.copyUp(name)
}

// Commit publishes all staged changes to the base filesystem as a single
// atomic batch. If any path the transaction read or modified changed in the
// base since it was first observed, the whole commit is rejected with a
// *TxnConflictError and the transaction stays active (it can be rolled back
// or inspected). After a successful commit the transaction is sealed;
// further commits, rollbacks or mutations return ErrTxnCommitted.
func (t *TxnOverlayFs) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return ErrTxnCommitted
	}
	committer, ok := t.base.(TxnCommitter)
	if !ok {
		return errors.New("afero: TxnOverlayFs base does not support atomic commits (TxnCommitter)")
	}
	ops, err := t.buildOpsLocked()
	if err != nil {
		return err
	}
	if err := committer.CommitTxn(t.versions, ops); err != nil {
		return err
	}
	t.committed = true
	return nil
}

// buildOpsLocked translates the staged transaction state into an ordered
// batch of operations for the base. The caller must hold t.mu.
func (t *TxnOverlayFs) buildOpsLocked() ([]TxnOp, error) {
	var ops []TxnOp

	// 1. Removals for whiteouts that were not consumed as rename sources.
	var removals []string
	for p := range t.whiteouts {
		if t.renameSources[p] {
			continue
		}
		removals = append(removals, p)
	}
	sort.Strings(removals)
	for _, p := range removals {
		// Skip entries already covered by an ancestor removal.
		covered := false
		for q := parentDir(p); ; q = parentDir(q) {
			if _, ok := t.whiteouts[q]; ok && !t.renameSources[q] {
				covered = true
				break
			}
			if parentDir(q) == q {
				break
			}
		}
		if covered {
			continue
		}
		if t.whiteouts[p] {
			ops = append(ops, TxnOp{Kind: TxnOpRemoveAll, Path: p})
		} else {
			ops = append(ops, TxnOp{Kind: TxnOpRemove, Path: p})
		}
	}

	// 2. Renames, in the order they were staged.
	ops = append(ops, t.renames...)

	// 3.+4. Directories and files staged in the layer.
	var dirs, files []string
	err := Walk(t.layer, FilePathSeparator, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if normalizePath(p) == FilePathSeparator {
			return nil
		}
		if fi.IsDir() {
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(dirs)
	sort.Strings(files)
	for _, d := range dirs {
		fi, err := t.layer.Stat(d)
		if err != nil {
			return nil, err
		}
		ops = append(ops, TxnOp{Kind: TxnOpMkdirAll, Path: d, Perm: fi.Mode().Perm()})
	}
	for _, f := range files {
		fi, err := t.layer.Stat(f)
		if err != nil {
			return nil, err
		}
		data, err := ReadFile(t.layer, f)
		if err != nil {
			return nil, err
		}
		ops = append(ops, TxnOp{
			Kind:  TxnOpWriteFile,
			Path:  f,
			Data:  data,
			Perm:  fi.Mode().Perm(),
			Mtime: fi.ModTime(),
		})
	}

	// 5. Metadata operations, in the order they were staged.
	ops = append(ops, t.metaOps...)

	return ops, nil
}

// Rollback discards all staged changes and resets the transaction to a
// fresh state. Afterwards the transaction view again matches the base view
// and the transaction can be reused. Rolling back a committed transaction
// returns ErrTxnCommitted.
func (t *TxnOverlayFs) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return ErrTxnCommitted
	}
	t.resetLocked()
	return nil
}

// txnDirFile is a directory handle over a merged directory snapshot.
type txnDirFile struct {
	name    string
	fi      os.FileInfo
	entries []os.FileInfo
	pos     int
	closed  bool
}

func (d *txnDirFile) Name() string { return d.name }

func (d *txnDirFile) Stat() (os.FileInfo, error) { return d.fi, nil }

func (d *txnDirFile) Sync() error { return nil }

func (d *txnDirFile) Close() error {
	d.closed = true
	return nil
}

func (d *txnDirFile) Readdir(count int) ([]os.FileInfo, error) {
	if d.closed {
		return nil, ErrFileClosed
	}
	if count <= 0 {
		res := d.entries[d.pos:]
		d.pos = len(d.entries)
		return res, nil
	}
	if d.pos >= len(d.entries) {
		return nil, io.EOF
	}
	end := d.pos + count
	if end > len(d.entries) {
		end = len(d.entries)
	}
	res := d.entries[d.pos:end]
	d.pos = end
	return res, nil
}

func (d *txnDirFile) Readdirnames(n int) ([]string, error) {
	fis, err := d.Readdir(n)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(fis))
	for i, fi := range fis {
		names[i] = fi.Name()
	}
	return names, nil
}

// ReadDir implements iofs.ReadDirFile.
func (d *txnDirFile) ReadDir(n int) ([]iofs.DirEntry, error) {
	fis, err := d.Readdir(n)
	if err != nil {
		return nil, err
	}
	entries := make([]iofs.DirEntry, len(fis))
	for i, fi := range fis {
		entries[i] = common.FileInfoDirEntry{FileInfo: fi}
	}
	return entries, nil
}

func (d *txnDirFile) dirErr(op string) error {
	return &os.PathError{Op: op, Path: d.name, Err: syscall.EISDIR}
}

func (d *txnDirFile) Read([]byte) (int, error)          { return 0, d.dirErr("read") }
func (d *txnDirFile) ReadAt([]byte, int64) (int, error) { return 0, d.dirErr("read") }
func (d *txnDirFile) Write([]byte) (int, error)         { return 0, d.dirErr("write") }
func (d *txnDirFile) WriteAt([]byte, int64) (int, error) {
	return 0, d.dirErr("write")
}
func (d *txnDirFile) WriteString(string) (int, error) { return 0, d.dirErr("write") }
func (d *txnDirFile) Truncate(int64) error            { return d.dirErr("truncate") }

func (d *txnDirFile) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekStart {
		d.pos = 0
		return 0, nil
	}
	return 0, d.dirErr("seek")
}
