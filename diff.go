// Copyright © 2026 Afero Authors.
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
	"path"
	"sort"
)

// DiffType describes how a relative path differs between the two compared
// directory trees.
type DiffType int

const (
	// DiffAdded marks a path that exists only in the "new" tree.
	DiffAdded DiffType = iota
	// DiffMissing marks a path that exists only in the "base" tree.
	DiffMissing
	// DiffTypeConflict marks a path whose node kind (file, directory,
	// symlink, ...) differs between the two trees.
	DiffTypeConflict
	// DiffContentChanged marks two regular files (or two symlinks) whose
	// bytes or link targets differ.
	DiffContentChanged
	// DiffUnstable marks entries that changed while they were being read:
	// the comparator cannot prove them equal, so it never reports equality
	// for a mutating file.
	DiffUnstable
	// DiffUnsupported marks special node kinds that the comparator does not
	// compare (named pipes, sockets, devices, ...). The entry is reported
	// explicitly instead of being silently ignored or misclassified.
	DiffUnsupported
)

// String returns the stable, lowercase identifier used when printing diffs.
func (d DiffType) String() string {
	switch d {
	case DiffAdded:
		return "added"
	case DiffMissing:
		return "missing"
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

// NodeType is the portable classification of a tree entry.
type NodeType int

const (
	// NodeAbsent means the path does not exist.
	NodeAbsent NodeType = iota
	// NodeFile is a regular file.
	NodeFile
	// NodeDir is a directory.
	NodeDir
	// NodeSymlink is a symbolic link, observed without following it.
	NodeSymlink
	// NodeUnsupported is any other mode (named pipe, socket, device, ...).
	NodeUnsupported
)

// ClassifyNode maps an os.FileInfo mode to one of the portable NodeType
// values. Symlinks can only be observed when the filesystem implements
// Lstater; filesystems that transparently follow links report the mode of
// the target instead.
func ClassifyNode(fi os.FileInfo) NodeType {
	if fi == nil {
		return NodeAbsent
	}
	m := fi.Mode()
	switch {
	case m&os.ModeSymlink != 0:
		return NodeSymlink
	case m.IsDir():
		return NodeDir
	case m.IsRegular():
		return NodeFile
	default:
		return NodeUnsupported
	}
}

// String returns a stable lowercase name for the node type.
func (n NodeType) String() string {
	switch n {
	case NodeAbsent:
		return "absent"
	case NodeFile:
		return "file"
	case NodeDir:
		return "dir"
	case NodeSymlink:
		return "symlink"
	case NodeUnsupported:
		return "unsupported"
	default:
		return "unknown"
	}
}

// Diff is a single difference found at RelPath.
type Diff struct {
	// RelPath is the path relative to each compared root. It always uses
	// forward slashes; the empty string denotes the root itself.
	RelPath string
	// Type is the kind of difference.
	Type DiffType
	// BaseType is the node kind in the base tree (NodeAbsent for DiffAdded).
	BaseType NodeType
	// NewType is the node kind in the new tree (NodeAbsent for DiffMissing).
	NewType NodeType
	// BaseSize and NewSize are the regular file sizes captured before
	// reading, when applicable. They are -1 when not applicable.
	BaseSize, NewSize int64
	// Detail carries extra, non-localized information, for example the
	// unsupported mode bits, the changed symlink target, or an instability
	// explanation.
	Detail string
}

// String renders the diff in a stable, human-readable form, for example
//
//	content_changed  dir/note.txt  (file -> file, 5 -> 7 bytes)
//	type_conflict     x             (file -> dir)
func (d Diff) String() string {
	switch d.Type {
	case DiffAdded, DiffMissing:
		kind := d.NewType
		if d.Type == DiffMissing {
			kind = d.BaseType
		}
		return fmt.Sprintf("%-16s %s (%s)", d.Type, displayRel(d.RelPath), kind)
	case DiffTypeConflict, DiffUnsupported:
		return fmt.Sprintf("%-16s %s (%s -> %s%s)", d.Type, displayRel(d.RelPath),
			d.BaseType, d.NewType, detailSuffix(d.Detail))
	default:
		if d.BaseType == NodeSymlink || d.NewType == NodeSymlink {
			return fmt.Sprintf("%-16s %s (%s -> %s%s)", d.Type, displayRel(d.RelPath),
				d.BaseType, d.NewType, detailSuffix(d.Detail))
		}
		return fmt.Sprintf("%-16s %s (%s -> %s, %d -> %d bytes%s)", d.Type, displayRel(d.RelPath),
			d.BaseType, d.NewType, d.BaseSize, d.NewSize, detailSuffix(d.Detail))
	}
}

func displayRel(rel string) string {
	if rel == "" {
		return "."
	}
	return rel
}

func detailSuffix(detail string) string {
	if detail == "" {
		return ""
	}
	return ", " + detail
}

// defaultChunkSize is the block size used for chunked content comparison.
// Files are streamed in blocks of this size and are never fully loaded into
// memory.
const defaultChunkSize = 64 * 1024

// DirDiffer compares two Afero directory trees without mutating either one:
// it never creates, copies, renames or deletes files. File contents are
// compared through per-block digests, so memory use is bounded by the chunk
// size regardless of total file size. Modification time alone is never
// treated as proof of a difference.
type DirDiffer struct {
	chunkSize int
	newHash   func() hash.Hash
}

// DifferOption customizes a DirDiffer.
type DifferOption func(*DirDiffer)

// WithChunkSize sets the read buffer / hashing block size. Non-positive
// values keep the default.
func WithChunkSize(n int) DifferOption {
	return func(d *DirDiffer) {
		if n > 0 {
			d.chunkSize = n
		}
	}
}

// WithHash replaces the per-chunk digest function (sha256 by default).
func WithHash(h func() hash.Hash) DifferOption {
	return func(d *DirDiffer) {
		if h != nil {
			d.newHash = h
		}
	}
}

// NewDirDiffer builds a read-only tree comparator.
func NewDirDiffer(opts ...DifferOption) *DirDiffer {
	d := &DirDiffer{chunkSize: defaultChunkSize, newHash: sha256.New}
	for _, o := range opts {
		o(d)
	}
	return d
}

// DiffTrees compares the directory tree rooted at baseRoot on baseFs with the
// tree rooted at newRoot on newFs. The two filesystems may be the same object.
// The returned slice is stably sorted by relative path, then by diff type.
//
// I/O errors encountered while walking or reading entries are returned to the
// caller. A file modified mid-comparison is reported as DiffUnstable rather
// than as an error or, worse, as equal.
func (d *DirDiffer) DiffTrees(baseFs Fs, baseRoot string, newFs Fs, newRoot string) ([]Diff, error) {
	baseInfo, err := lstatEntry(baseFs, baseRoot)
	if err != nil {
		return nil, fmt.Errorf("diff: stat base root %q: %w", baseRoot, err)
	}
	newInfo, err := lstatEntry(newFs, newRoot)
	if err != nil {
		return nil, fmt.Errorf("diff: stat new root %q: %w", newRoot, err)
	}

	baseType := ClassifyNode(baseInfo)
	newType := ClassifyNode(newInfo)

	if baseType != NodeDir || newType != NodeDir {
		diff, cmpErr := d.compareNode("",
			baseFs, baseRoot, baseInfo, baseType,
			newFs, newRoot, newInfo, newType)
		if cmpErr != nil {
			return nil, cmpErr
		}
		if diff != nil {
			return []Diff{*diff}, nil
		}
		return nil, nil
	}

	baseNodes, err := d.snapshotTree(baseFs, baseRoot)
	if err != nil {
		return nil, err
	}
	newNodes, err := d.snapshotTree(newFs, newRoot)
	if err != nil {
		return nil, err
	}

	relSet := make(map[string]struct{}, len(baseNodes)+len(newNodes))
	for rel := range baseNodes {
		relSet[rel] = struct{}{}
	}
	for rel := range newNodes {
		relSet[rel] = struct{}{}
	}
	rels := make([]string, 0, len(relSet))
	for rel := range relSet {
		rels = append(rels, rel)
	}
	sort.Strings(rels)

	// A type conflict at a path shadows everything beneath it: descent is
	// impossible on the side whose node is not a directory, so children are
	// not reported separately (which would only produce noise).
	conflicts := map[string]struct{}{}
	for _, rel := range rels {
		bn, bok := baseNodes[rel]
		nn, nok := newNodes[rel]
		if bok && nok && bn.typ != nn.typ {
			conflicts[rel] = struct{}{}
		}
	}

	var out []Diff
	for _, rel := range rels {
		if ancestorConflicts(rel, conflicts) {
			continue
		}
		bn := baseNodes[rel]
		nn := newNodes[rel]

		var (
			bi, ni os.FileInfo
			bt, nt = NodeAbsent, NodeAbsent
			bp, np string
		)
		if bn != nil {
			bi, bt, bp = bn.info, bn.typ, bn.abs
		}
		if nn != nil {
			ni, nt, np = nn.info, nn.typ, nn.abs
		}
		diff, cmpErr := d.compareNode(rel,
			baseFs, bp, bi, bt,
			newFs, np, ni, nt)
		if cmpErr != nil {
			return nil, cmpErr
		}
		if diff != nil {
			out = append(out, *diff)
		}
	}

	// Stable sort: relative path first, diff type second. Ties keep the
	// deterministic lexical order produced above.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].RelPath != out[j].RelPath {
			return out[i].RelPath < out[j].RelPath
		}
		return out[i].Type < out[j].Type
	})
	return out, nil
}

// DiffTrees compares two trees with a default comparator. See DirDiffer.
func DiffTrees(baseFs Fs, baseRoot string, newFs Fs, newRoot string) ([]Diff, error) {
	return NewDirDiffer().DiffTrees(baseFs, baseRoot, newFs, newRoot)
}

// ancestorConflicts reports whether any ancestor directory of rel (rel
// itself excluded) is a known type conflict.
func ancestorConflicts(rel string, conflicts map[string]struct{}) bool {
	for dir := path.Dir(rel); dir != "." && dir != "/"; dir = path.Dir(dir) {
		if _, ok := conflicts[dir]; ok {
			return true
		}
	}
	return false
}

type nodeInfo struct {
	abs  string // path inside its filesystem
	rel  string // slash-separated path relative to the compared root
	info os.FileInfo
	typ  NodeType
}

// snapshotTree walks root lexicographically and depth-first, collecting lstat
// information for every entry except root itself. Relative names are built by
// joining name components while descending (never by trimming a string
// prefix), which keeps similarly prefixed directories such as /a and /ab
// apart.
func (d *DirDiffer) snapshotTree(fsys Fs, root string) (map[string]*nodeInfo, error) {
	nodes := map[string]*nodeInfo{}

	var walk func(dirAbs, dirRel string) error
	walk = func(dirAbs, dirRel string) error {
		f, err := fsys.Open(dirAbs)
		if err != nil {
			return fmt.Errorf("diff: open directory %q: %w", dirAbs, err)
		}
		names, err := f.Readdirnames(-1)
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			return fmt.Errorf("diff: read directory %q: %w", dirAbs, err)
		}
		sort.Strings(names)

		for _, name := range names {
			childAbs := path.Join(dirAbs, name)
			childRel := name
			if dirRel != "" {
				childRel = path.Join(dirRel, name)
			}
			fi, err := lstatEntry(fsys, childAbs)
			if err != nil {
				return fmt.Errorf("diff: lstat %q: %w", childAbs, err)
			}
			typ := ClassifyNode(fi)
			nodes[childRel] = &nodeInfo{abs: childAbs, rel: childRel, info: fi, typ: typ}
			if typ == NodeDir {
				if err := walk(childAbs, childRel); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walk(root, ""); err != nil {
		return nil, err
	}
	return nodes, nil
}

// lstatEntry returns file info without following symlinks when the filesystem
// implements Lstater; otherwise it falls back to Stat.
func lstatEntry(fsys Fs, name string) (os.FileInfo, error) {
	if ls, ok := fsys.(Lstater); ok {
		fi, _, err := ls.LstatIfPossible(name)
		return fi, err
	}
	return fsys.Stat(name)
}

// compareNode compares one relative path across the two trees and returns
// the diff (nil when the entries are equal).
func (d *DirDiffer) compareNode(
	rel string,
	baseFs Fs, baseAbs string, baseInfo os.FileInfo, baseType NodeType,
	newFs Fs, newAbs string, newInfo os.FileInfo, newType NodeType,
) (*Diff, error) {
	switch {
	case baseType == NodeAbsent && newType == NodeAbsent:
		return nil, nil
	case baseType == NodeAbsent:
		return &Diff{
			RelPath: rel, Type: DiffAdded, BaseType: NodeAbsent, NewType: newType,
			BaseSize: -1, NewSize: sizeOf(newInfo, newType),
		}, nil
	case newType == NodeAbsent:
		return &Diff{
			RelPath: rel, Type: DiffMissing, BaseType: baseType, NewType: NodeAbsent,
			BaseSize: sizeOf(baseInfo, baseType), NewSize: -1,
		}, nil
	}

	if baseType != newType {
		return &Diff{
			RelPath: rel, Type: DiffTypeConflict, BaseType: baseType, NewType: newType,
			BaseSize: sizeOf(baseInfo, baseType), NewSize: sizeOf(newInfo, newType),
		}, nil
	}

	switch baseType {
	case NodeDir:
		// Directories are compared through their children; directory
		// mode and modification time are intentionally ignored.
		return nil, nil
	case NodeUnsupported:
		return &Diff{
			RelPath: rel, Type: DiffUnsupported, BaseType: baseType, NewType: newType,
			BaseSize: -1, NewSize: -1,
			Detail: "node mode " + baseInfo.Mode().String() + " is not supported for comparison",
		}, nil
	case NodeSymlink:
		return d.compareSymlinks(rel, baseFs, baseAbs, baseInfo, newFs, newAbs, newInfo)
	case NodeFile:
		return d.compareFiles(rel, baseFs, baseAbs, baseInfo, newFs, newAbs, newInfo)
	default:
		return nil, nil
	}
}

func sizeOf(fi os.FileInfo, t NodeType) int64 {
	if t == NodeFile && fi != nil {
		return fi.Size()
	}
	return -1
}

// compareSymlinks compares symlink targets. Reaching NodeSymlink requires
// lstat semantics; resolving the target additionally requires LinkReader.
func (d *DirDiffer) compareSymlinks(
	rel string,
	baseFs Fs, baseAbs string, baseInfo os.FileInfo,
	newFs Fs, newAbs string, newInfo os.FileInfo,
) (*Diff, error) {
	bReader, bOK := baseFs.(LinkReader)
	nReader, nOK := newFs.(LinkReader)
	if !bOK || !nOK {
		return &Diff{
			RelPath: rel, Type: DiffUnsupported, BaseType: NodeSymlink, NewType: NodeSymlink,
			BaseSize: -1, NewSize: -1, Detail: "filesystem cannot read link targets",
		}, nil
	}

	bTarget, err := bReader.ReadlinkIfPossible(baseAbs)
	if err != nil {
		return nil, fmt.Errorf("diff: readlink %q: %w", baseAbs, err)
	}
	nTarget, err := nReader.ReadlinkIfPossible(newAbs)
	if err != nil {
		return nil, fmt.Errorf("diff: readlink %q: %w", newAbs, err)
	}

	// Re-stat after reading: if the inode changed underneath us, do not
	// pretend the comparison is trustworthy.
	bAfter, err := lstatEntry(baseFs, baseAbs)
	if err != nil {
		return nil, fmt.Errorf("diff: re-stat %q: %w", baseAbs, err)
	}
	nAfter, err := lstatEntry(newFs, newAbs)
	if err != nil {
		return nil, fmt.Errorf("diff: re-stat %q: %w", newAbs, err)
	}
	if !sameMeta(baseInfo, bAfter) || !sameMeta(newInfo, nAfter) {
		return &Diff{
			RelPath: rel, Type: DiffUnstable, BaseType: NodeSymlink, NewType: NodeSymlink,
			BaseSize: -1, NewSize: -1, Detail: "symlink metadata changed during comparison",
		}, nil
	}

	if bTarget != nTarget {
		return &Diff{
			RelPath: rel, Type: DiffContentChanged, BaseType: NodeSymlink, NewType: NodeSymlink,
			BaseSize: -1, NewSize: -1,
			Detail: fmt.Sprintf("link target %q -> %q", bTarget, nTarget),
		}, nil
	}
	return nil, nil
}

// compareFiles compares two regular files via chunked streaming digests.
// Sizes are compared before reading and, together with mode and modification
// time, re-checked afterwards: any concurrent modification yields
// DiffUnstable instead of equality.
func (d *DirDiffer) compareFiles(
	rel string,
	baseFs Fs, baseAbs string, baseInfo os.FileInfo,
	newFs Fs, newAbs string, newInfo os.FileInfo,
) (*Diff, error) {
	bBefore := metaOf(baseInfo)
	nBefore := metaOf(newInfo)

	// Files with different sizes cannot have identical contents, so no
	// bytes need to be read.
	if bBefore.size != nBefore.size {
		return &Diff{
			RelPath: rel, Type: DiffContentChanged, BaseType: NodeFile, NewType: NodeFile,
			BaseSize: bBefore.size, NewSize: nBefore.size,
		}, nil
	}

	bf, err := baseFs.Open(baseAbs)
	if err != nil {
		return nil, fmt.Errorf("diff: open %q: %w", baseAbs, err)
	}
	defer bf.Close()
	nf, err := newFs.Open(newAbs)
	if err != nil {
		return nil, fmt.Errorf("diff: open %q: %w", newAbs, err)
	}
	defer nf.Close()

	equal, err := d.streamEqual(bf, nf)
	if err != nil {
		return nil, err
	}

	// Verify size and metadata again after reading. A fresh lstat also
	// detects node replacement (including replacement by a symlink).
	bAfter, err := statAfterOpen(baseFs, baseAbs, bf)
	if err != nil {
		return nil, fmt.Errorf("diff: re-stat %q: %w", baseAbs, err)
	}
	nAfter, err := statAfterOpen(newFs, newAbs, nf)
	if err != nil {
		return nil, fmt.Errorf("diff: re-stat %q: %w", newAbs, err)
	}

	if bBefore != metaOf(bAfter) || nBefore != metaOf(nAfter) {
		return &Diff{
			RelPath: rel, Type: DiffUnstable, BaseType: NodeFile, NewType: NodeFile,
			BaseSize: bBefore.size, NewSize: nBefore.size,
			Detail: fmt.Sprintf("file changed during read (size %d -> %d / %d -> %d)",
				bBefore.size, metaOf(bAfter).size, nBefore.size, metaOf(nAfter).size),
		}, nil
	}

	if !equal {
		return &Diff{
			RelPath: rel, Type: DiffContentChanged, BaseType: NodeFile, NewType: NodeFile,
			BaseSize: bBefore.size, NewSize: nBefore.size,
		}, nil
	}
	return nil, nil
}

// statAfterOpen re-checks metadata after a streaming read, preferring lstat
// semantics so the post-read fingerprint matches the pre-read one and symlink
// replacement is visible. If the node disappeared, the open handle's own stat
// is used (it will mismatch the pre-read metadata and surface as unstable).
func statAfterOpen(fsys Fs, abs string, f File) (os.FileInfo, error) {
	fi, err := lstatEntry(fsys, abs)
	if err == nil {
		return fi, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return f.Stat()
}

// metaSig is the pre/post-read metadata fingerprint used to detect files
// mutated while they were being read.
type metaSig struct {
	size  int64
	mode  os.FileMode
	mtime int64 // unix nano
	isDir bool
}

func metaOf(fi os.FileInfo) metaSig {
	return metaSig{
		size:  fi.Size(),
		mode:  fi.Mode(),
		mtime: fi.ModTime().UnixNano(),
		isDir: fi.IsDir(),
	}
}

func sameMeta(a, b os.FileInfo) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return metaOf(a) == metaOf(b)
}

// streamEqual compares two readers block by block while never holding more
// than two chunk-sized buffers. Per-block digests are compared so mismatching
// blocks are detected early without buffering the whole files.
func (d *DirDiffer) streamEqual(a, b io.Reader) (bool, error) {
	ba := make([]byte, d.chunkSize)
	bb := make([]byte, d.chunkSize)
	ha := d.newHash()
	hb := d.newHash()
	var totalA, totalB int64
	for {
		na, erra := io.ReadFull(a, ba)
		nb, errb := io.ReadFull(b, bb)
		if erra != nil && erra != io.EOF && erra != io.ErrUnexpectedEOF {
			return false, erra
		}
		if errb != nil && errb != io.EOF && errb != io.ErrUnexpectedEOF {
			return false, errb
		}
		if na > 0 {
			ha.Reset()
			ha.Write(ba[:na])
			totalA += int64(na)
		}
		if nb > 0 {
			hb.Reset()
			hb.Write(bb[:nb])
			totalB += int64(nb)
		}
		if na != nb || (na > 0 && !bytes.Equal(ha.Sum(nil), hb.Sum(nil))) {
			return false, nil
		}
		if erra == io.EOF || errb == io.EOF {
			// Both streams must finish at the same time.
			return erra == errb && totalA == totalB, nil
		}
	}
}
