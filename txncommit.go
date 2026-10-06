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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero/mem"
)

// ErrTxnConflict is returned (wrapped by TxnConflictError) when a
// transactional commit is rejected because the base filesystem changed
// underneath the transaction.
var ErrTxnConflict = errors.New("afero: transaction conflict")

// ErrTxnCommitted is returned when an operation is attempted on a
// transaction that has already been committed.
var ErrTxnCommitted = errors.New("afero: transaction already committed")

// TxnConflictError describes a version mismatch detected while committing
// a transaction: the named path was modified in the base filesystem after
// the transaction first observed it.
type TxnConflictError struct {
	Path     string
	Expected uint64
	Actual   uint64
}

func (e *TxnConflictError) Error() string {
	return fmt.Sprintf("%v: base path %q changed underneath the transaction (recorded version %d, current version %d)",
		ErrTxnConflict, e.Path, e.Expected, e.Actual)
}

// Is reports whether target is ErrTxnConflict.
func (e *TxnConflictError) Is(target error) bool { return target == ErrTxnConflict }

// TxnOpKind identifies the kind of a TxnOp.
type TxnOpKind int

const (
	// TxnOpMkdirAll creates a directory and all missing parents.
	TxnOpMkdirAll TxnOpKind = iota
	// TxnOpWriteFile atomically replaces a file with the given content.
	TxnOpWriteFile
	// TxnOpRemove removes a single file or empty directory.
	TxnOpRemove
	// TxnOpRemoveAll removes a path and all its children.
	TxnOpRemoveAll
	// TxnOpRename renames Path to Target, moving any descendants along.
	TxnOpRename
	// TxnOpChmod changes the mode of a path.
	TxnOpChmod
	// TxnOpChtimes changes the times of a path.
	TxnOpChtimes
	// TxnOpChown changes the ownership of a path.
	TxnOpChown
)

// TxnOp is a single mutation applied atomically by CommitTxn.
type TxnOp struct {
	Kind TxnOpKind
	// Path is the subject of the operation (the source for TxnOpRename).
	Path string
	// Target is the destination for TxnOpRename.
	Target string
	// Data is the new file content for TxnOpWriteFile.
	Data []byte
	// Perm is the permission for TxnOpMkdirAll, TxnOpWriteFile and TxnOpChmod.
	Perm os.FileMode
	// Atime and Mtime are used by TxnOpWriteFile (Mtime only) and TxnOpChtimes.
	Atime time.Time
	Mtime time.Time
	// Uid and Gid are used by TxnOpChown.
	Uid int
	Gid int
}

// TxnCommitter is implemented by filesystems that can validate a set of
// version preconditions and apply a batch of operations atomically: outside
// observers either see none or all of the operations.
type TxnCommitter interface {
	// CommitTxn first verifies that for every path in checks the current
	// version equals the expected one. If any check fails, nothing is
	// applied and a *TxnConflictError is returned. Otherwise all ops are
	// applied as a single atomic batch.
	CommitTxn(checks map[string]uint64, ops []TxnOp) error
}

// versionedFs is implemented by filesystems that track per-path versions.
type versionedFs interface {
	// PathVersion returns the current version of name, or 0 if the path
	// was never modified through the filesystem.
	PathVersion(name string) uint64
}

// parentDir returns the normalized parent directory of name.
func parentDir(name string) string {
	return normalizePath(filepath.Dir(filepath.Clean(name)))
}

// bumpVersionsLocked assigns a fresh version to all given paths.
// The caller must hold m.mu (or otherwise guarantee exclusive access).
func (m *MemMapFs) bumpVersionsLocked(names ...string) {
	m.getData()
	m.vc++
	for _, n := range names {
		m.versions[normalizePath(n)] = m.vc
	}
}

// PathVersion implements versionedFs.
func (m *MemMapFs) PathVersion(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.getData()
	return m.versions[normalizePath(name)]
}

// CommitTxn implements TxnCommitter. The whole check-and-apply sequence is
// performed while holding the filesystem lock, so concurrent readers either
// observe the state before or after the batch, never a mixture.
func (m *MemMapFs) CommitTxn(checks map[string]uint64, ops []TxnOp) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getData()

	for p, v := range checks {
		if actual := m.versions[normalizePath(p)]; actual != v {
			return &TxnConflictError{Path: p, Expected: v, Actual: actual}
		}
	}

	// Pre-validate the batch so that the apply phase below cannot fail
	// half-way through: simulate the operations on the set of known paths
	// and make sure every rename source exists at its point in the batch.
	if err := m.validateTxnOpsLocked(ops); err != nil {
		return err
	}

	for _, op := range ops {
		m.applyTxnOpLocked(op)
	}
	return nil
}

// validateTxnOpsLocked simulates a batch of operations on the set of
// existing paths, verifying that every rename source exists at the moment
// the rename would be applied. The caller must hold m.mu.
func (m *MemMapFs) validateTxnOpsLocked(ops []TxnOp) error {
	paths := make(map[string]bool, len(m.getData()))
	for p := range m.getData() {
		paths[p] = true
	}
	for _, op := range ops {
		switch op.Kind {
		case TxnOpRename:
			oldname := normalizePath(op.Path)
			if !paths[oldname] {
				return &os.PathError{Op: "rename", Path: op.Path, Err: ErrFileNotFound}
			}
			newname := normalizePath(op.Target)
			var moved []string
			for p := range paths {
				if p == oldname || strings.HasPrefix(p, oldname+FilePathSeparator) {
					moved = append(moved, p)
				}
			}
			for _, p := range moved {
				delete(paths, p)
				paths[newname+strings.TrimPrefix(p, oldname)] = true
			}
		case TxnOpRemove, TxnOpRemoveAll:
			name := normalizePath(op.Path)
			for p := range paths {
				if p == name || strings.HasPrefix(p, name+FilePathSeparator) {
					delete(paths, p)
				}
			}
		case TxnOpMkdirAll, TxnOpWriteFile:
			paths[normalizePath(op.Path)] = true
		}
	}
	return nil
}

// applyTxnOpLocked applies a single operation. The caller must hold m.mu
// and must have pre-validated the batch.
func (m *MemMapFs) applyTxnOpLocked(op TxnOp) {
	switch op.Kind {
	case TxnOpMkdirAll:
		name := normalizePath(op.Path)
		if _, ok := m.getData()[name]; ok {
			return
		}
		_ = m.lockfreeMkdir(name, op.Perm)
		m.bumpVersionsLocked(name, parentDir(name))

	case TxnOpWriteFile:
		name := normalizePath(op.Path)
		if _, ok := m.getData()[name]; ok {
			// Replace (rather than mutate) the file data so that
			// already open handles keep observing the old content and
			// new observers atomically see the new content.
			_ = m.unRegisterWithParent(name)
			delete(m.getData(), name)
		}
		perm := op.Perm
		if perm == 0 {
			perm = 0o666
		}
		fd := mem.CreateFile(name)
		mem.SetMode(fd, perm)
		m.getData()[name] = fd
		m.registerWithParent(fd, perm)
		if len(op.Data) > 0 {
			h := mem.NewFileHandle(fd)
			_, _ = h.Write(op.Data)
			_ = h.Close()
		}
		if !op.Mtime.IsZero() {
			mem.SetModTime(fd, op.Mtime)
		}
		m.bumpVersionsLocked(name, parentDir(name))

	case TxnOpRemove:
		name := normalizePath(op.Path)
		if _, ok := m.getData()[name]; !ok {
			return
		}
		_ = m.unRegisterWithParent(name)
		delete(m.getData(), name)
		m.bumpVersionsLocked(name, parentDir(name))

	case TxnOpRemoveAll:
		m.lockfreeRemoveAllLocked(op.Path)

	case TxnOpRename:
		m.lockfreeRenameLocked(op.Path, op.Target)

	case TxnOpChmod:
		name := normalizePath(op.Path)
		f, ok := m.getData()[name]
		if !ok {
			return
		}
		prevOtherBits := mem.GetFileInfo(f).Mode() &^ chmodBits
		mem.SetMode(f, prevOtherBits|(op.Perm&chmodBits))
		m.bumpVersionsLocked(name)

	case TxnOpChtimes:
		name := normalizePath(op.Path)
		f, ok := m.getData()[name]
		if !ok {
			return
		}
		mem.SetModTime(f, op.Mtime)
		m.bumpVersionsLocked(name)

	case TxnOpChown:
		name := normalizePath(op.Path)
		f, ok := m.getData()[name]
		if !ok {
			return
		}
		mem.SetUID(f, op.Uid)
		mem.SetGID(f, op.Gid)
		m.bumpVersionsLocked(name)
	}
}
