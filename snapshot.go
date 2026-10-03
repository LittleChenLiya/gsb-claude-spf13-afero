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
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// DefaultBlockSize is the default size in bytes of the chunks used to digest
// file contents. Files are never held in their entirety: each chunk is fed to
// the hash as it is read, so memory use stays bounded regardless of file size.
const DefaultBlockSize = 32 * 1024

// NodeKind identifies the type of a filesystem node captured by a Snapshot.
type NodeKind int

const (
	// NodeKindFile is a regular file.
	NodeKindFile NodeKind = iota
	// NodeKindDir is a directory.
	NodeKindDir
	// NodeKindSymlink is a symbolic link. Links are never followed during a
	// snapshot; the link itself is recorded.
	NodeKindSymlink
	// NodeKindSpecial is an unsupported special node (device, pipe, socket,
	// or a node that could not be classified or read). Special nodes are
	// reported explicitly rather than treated as equal.
	NodeKindSpecial
)

// String returns the lowercase name of the node kind.
func (k NodeKind) String() string {
	switch k {
	case NodeKindFile:
		return "file"
	case NodeKindDir:
		return "dir"
	case NodeKindSymlink:
		return "symlink"
	case NodeKindSpecial:
		return "special"
	default:
		return "unknown"
	}
}

// DiffKind identifies the kind of difference between two snapshots.
type DiffKind int

const (
	// DiffAdded: the path exists only in the new snapshot.
	DiffAdded DiffKind = iota
	// DiffRemoved: the path exists only in the old snapshot.
	DiffRemoved
	// DiffTypeConflict: the path exists in both snapshots but as different
	// node kinds (e.g. a file in one and a directory in the other).
	DiffTypeConflict
	// DiffContentChanged: both sides are files (or symlinks) with different
	// content. Same mtime and length do not mask a content difference.
	DiffContentChanged
	// DiffUnstable: a file changed while it was being read, so equality cannot
	// be established. Unstable files are never reported as equal.
	DiffUnstable
	// DiffUnsupported: a special node cannot be content-compared.
	DiffUnsupported
)

// String returns the lowercase name of the diff kind.
func (k DiffKind) String() string {
	switch k {
	case DiffAdded:
		return "added"
	case DiffRemoved:
		return "removed"
	case DiffTypeConflict:
		return "type_conflict"
	case DiffContentChanged:
		return "content_changed"
	case DiffUnstable:
		return "unstable"
	case DiffUnsupported:
		return "unsupported"
	default:
		return "unknown"
	}
}

// NodeSnapshot is the captured, read-only state of a single path within a
// Snapshot.
type NodeSnapshot struct {
	// Kind is the node type (file, dir, symlink, special).
	Kind NodeKind
	// Size is the node size captured before reading (for reporting).
	Size int64
	// Mode is the node permission/mode bits captured before reading.
	Mode os.FileMode
	// ModTime is the modification time captured before reading.
	ModTime time.Time
	// Link is the symlink target (only for NodeKindSymlink).
	Link string
	// Digest is the chunked content digest (only for NodeKindFile).
	Digest []byte
	// ReadErr records an error encountered while reading or stating the node.
	ReadErr error
	// Unstable reports whether the file changed while it was being read.
	Unstable bool
}

// Snapshot is a read-only, point-in-time view of a directory tree.
//
// A snapshot records the kind and a chunked content digest of every node
// beneath a root. It never writes to, copies, or deletes any files. Create
// one with NewSnapshot and compare two with Diff.
type Snapshot struct {
	root    string
	entries map[string]*NodeSnapshot
}

// SnapshotOptions configures snapshot creation.
type SnapshotOptions struct {
	// BlockSize is the read buffer size used when digesting file contents, in
	// bytes. If <= 0, DefaultBlockSize is used.
	BlockSize int
	// Hash returns the hash used for content digests. Defaults to sha256.New.
	Hash func() hash.Hash
}

// NewSnapshot captures a read-only snapshot of the tree rooted at root in fs.
//
// The snapshot is read-only: it wraps fs in a ReadOnlyFs and only ever stats
// and opens files for reading. File contents are digested in bounded chunks,
// so the whole file is never loaded into memory. The size and modification
// time of each file are checked before and after reading; a file that changes
// during reading is marked unstable.
func NewSnapshot(fs Fs, root string, opts ...SnapshotOptions) (*Snapshot, error) {
	blockSize := DefaultBlockSize
	var h hash.Hash = sha256.New()
	if len(opts) > 0 {
		if opts[0].BlockSize > 0 {
			blockSize = opts[0].BlockSize
		}
		if opts[0].Hash != nil {
			h = opts[0].Hash()
		}
	}

	root = filepath.Clean(root)
	info, err := fs.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &os.PathError{Op: "snapshot", Path: root, Err: errors.New("not a directory")}
	}

	// Enforce read-only access for the entire walk.
	ro := NewReadOnlyFs(fs)

	s := &Snapshot{root: root, entries: make(map[string]*NodeSnapshot)}

	walkErr := Walk(ro, root, func(path string, fi os.FileInfo, werr error) error {
		if werr != nil {
			if path == root {
				return werr
			}
			// A node that cannot be stated/read is recorded as special and
			// reported explicitly rather than silently dropped.
			s.entries[relPath(root, path)] = &NodeSnapshot{Kind: NodeKindSpecial, ReadErr: werr}
			return nil
		}
		if path == root {
			return nil
		}
		s.entries[relPath(root, path)] = snapshotNode(ro, path, fi, blockSize, h)
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return s, nil
}

// Paths returns the sorted relative paths captured in the snapshot.
func (s *Snapshot) Paths() []string {
	paths := make([]string, 0, len(s.entries))
	for p := range s.entries {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

// Get returns the captured node for the given relative path, or nil.
func (s *Snapshot) Get(rel string) *NodeSnapshot {
	return s.entries[rel]
}

// Diff is a single difference between two snapshots.
type Diff struct {
	// Path is the relative path within the snapshot roots.
	Path string
	// Kind is the difference kind.
	Kind DiffKind
	// Old is the node in the old snapshot (nil for added paths).
	Old *NodeSnapshot
	// New is the node in the new snapshot (nil for removed paths).
	New *NodeSnapshot
}

// String renders the diff as a single human-readable line.
func (d Diff) String() string {
	marker := map[DiffKind]string{
		DiffAdded:          "+",
		DiffRemoved:        "-",
		DiffTypeConflict:   "!",
		DiffContentChanged: "~",
		DiffUnstable:       "?",
		DiffUnsupported:    "#",
	}[d.Kind]
	if marker == "" {
		marker = "?"
	}
	detail := ""
	switch d.Kind {
	case DiffTypeConflict:
		oldKind, newKind := "<missing>", "<missing>"
		if d.Old != nil {
			oldKind = d.Old.Kind.String()
		}
		if d.New != nil {
			newKind = d.New.Kind.String()
		}
		detail = fmt.Sprintf(" (%s -> %s)", oldKind, newKind)
	case DiffUnstable, DiffUnsupported:
		if d.Old != nil && d.Old.ReadErr != nil {
			detail = fmt.Sprintf(" (old: %v)", d.Old.ReadErr)
		} else if d.New != nil && d.New.ReadErr != nil {
			detail = fmt.Sprintf(" (new: %v)", d.New.ReadErr)
		}
	}
	return fmt.Sprintf("%s %-15s %s%s", marker, d.Kind.String(), d.Path, detail)
}

// Diff computes the differences between s (the old snapshot) and other (the new
// snapshot). The result is stably sorted by path. Only differences are
// returned; equal nodes are omitted.
func (s *Snapshot) Diff(other *Snapshot) []Diff {
	keys := make([]string, 0, len(s.entries)+len(other.entries))
	seen := make(map[string]struct{}, len(s.entries)+len(other.entries))
	for k := range s.entries {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	for k := range other.entries {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	diffs := make([]Diff, 0)
	for _, k := range keys {
		oldNode, inOld := s.entries[k]
		newNode, inNew := other.entries[k]
		switch {
		case inOld && !inNew:
			diffs = append(diffs, Diff{Path: k, Kind: DiffRemoved, Old: oldNode})
		case !inOld && inNew:
			diffs = append(diffs, Diff{Path: k, Kind: DiffAdded, New: newNode})
		case inOld && inNew:
			if d := compareNodes(k, oldNode, newNode); d != nil {
				diffs = append(diffs, *d)
			}
		}
	}
	return diffs
}

// PrintDiffs writes a human-readable rendering of diffs to w, one per line.
func PrintDiffs(w io.Writer, diffs []Diff) {
	for _, d := range diffs {
		fmt.Fprintln(w, d.String())
	}
}

// relPath returns the path of p relative to root. It is boundary-aware: a
// root of "/a/foo" does not make "/a/foobar" a child of "/a/foo".
func relPath(root, p string) string {
	root = filepath.Clean(root)
	p = filepath.Clean(p)
	if p == root {
		return "."
	}
	sep := string(filepath.Separator)
	prefix := root
	if !strings.HasSuffix(prefix, sep) {
		prefix += sep
	}
	if strings.HasPrefix(p, prefix) {
		return p[len(prefix):]
	}
	if rel, err := filepath.Rel(root, p); err == nil {
		return rel
	}
	return p
}

// snapshotNode captures the state of a single node.
func snapshotNode(fs Fs, path string, fi os.FileInfo, blockSize int, h hash.Hash) *NodeSnapshot {
	node := &NodeSnapshot{
		Size:    fi.Size(),
		Mode:    fi.Mode(),
		ModTime: fi.ModTime(),
	}
	mode := fi.Mode()
	switch {
	case mode.IsDir():
		node.Kind = NodeKindDir
	case mode&os.ModeSymlink != 0:
		node.Kind = NodeKindSymlink
		if target, err := readlinkIfPossible(fs, path); err == nil {
			node.Link = target
		} else {
			node.ReadErr = err
		}
	case mode.IsRegular():
		node.Kind = NodeKindFile
		node.snapshotFile(fs, path, blockSize, h)
	default:
		node.Kind = NodeKindSpecial
	}
	return node
}

// snapshotFile digests a file's content in bounded chunks and verifies size
// and metadata before and after reading. A file that changes during reading is
// marked unstable.
func (node *NodeSnapshot) snapshotFile(fs Fs, path string, blockSize int, h hash.Hash) {
	fiBefore, err := fs.Stat(path)
	if err != nil {
		node.ReadErr = err
		return
	}
	sizeBefore := fiBefore.Size()
	modBefore := fiBefore.ModTime()

	digest, readErr := digestFile(fs, path, blockSize, h)
	node.Digest = digest
	if readErr != nil {
		node.ReadErr = readErr
	}

	fiAfter, err := fs.Stat(path)
	if err != nil {
		node.Unstable = true
		node.ReadErr = errors.Join(node.ReadErr, err)
		return
	}
	if sizeBefore != fiAfter.Size() || !modBefore.Equal(fiAfter.ModTime()) {
		node.Unstable = true
	}
}

// digestFile reads a file in chunks of blockSize, feeding each chunk to h. The
// whole file is never held in memory.
func digestFile(fs Fs, path string, blockSize int, h hash.Hash) ([]byte, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h.Reset()
	buf := make([]byte, blockSize)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if err != nil {
			if err == io.EOF {
				return h.Sum(nil), nil
			}
			return nil, err
		}
	}
}

// readlinkIfPossible returns the target of a symlink if the filesystem
// supports it.
func readlinkIfPossible(fs Fs, path string) (string, error) {
	if rl, ok := fs.(LinkReader); ok {
		return rl.ReadlinkIfPossible(path)
	}
	return "", &os.PathError{Op: "readlink", Path: path, Err: ErrNoReadlink}
}

// compareNodes compares two captured nodes at the same relative path.
func compareNodes(path string, a, b *NodeSnapshot) *Diff {
	if a.Kind != b.Kind {
		return &Diff{Path: path, Kind: DiffTypeConflict, Old: a, New: b}
	}
	switch a.Kind {
	case NodeKindDir:
		// Directories are equal by existence; their children are compared
		// individually as separate entries.
		return nil
	case NodeKindSymlink:
		if a.ReadErr != nil || b.ReadErr != nil {
			return &Diff{Path: path, Kind: DiffUnstable, Old: a, New: b}
		}
		if a.Link != b.Link {
			return &Diff{Path: path, Kind: DiffContentChanged, Old: a, New: b}
		}
		return nil
	case NodeKindFile:
		if a.Unstable || b.Unstable {
			return &Diff{Path: path, Kind: DiffUnstable, Old: a, New: b}
		}
		if a.ReadErr != nil || b.ReadErr != nil {
			return &Diff{Path: path, Kind: DiffUnstable, Old: a, New: b}
		}
		if !bytes.Equal(a.Digest, b.Digest) {
			return &Diff{Path: path, Kind: DiffContentChanged, Old: a, New: b}
		}
		return nil
	case NodeKindSpecial:
		return &Diff{Path: path, Kind: DiffUnsupported, Old: a, New: b}
	}
	return nil
}
