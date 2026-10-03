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
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------- test helpers ----------

func newTestFs(t *testing.T, root string) Fs {
	t.Helper()
	fs := NewMemMapFs()
	if err := fs.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("MkdirAll %q: %v", root, err)
	}
	return fs
}

func writeTestFile(t *testing.T, fs Fs, path string, data []byte) {
	t.Helper()
	if err := WriteFile(fs, path, data, 0o644); err != nil {
		t.Fatalf("WriteFile %q: %v", path, err)
	}
}

func mkdirTest(t *testing.T, fs Fs, path string) {
	t.Helper()
	if err := fs.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("MkdirAll %q: %v", path, err)
	}
}

func diffKinds(diffs []Diff) map[string]DiffKind {
	out := make(map[string]DiffKind, len(diffs))
	for _, d := range diffs {
		out[d.Path] = d.Kind
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------- counting reader (block size + error tests) ----------

type countingFile struct {
	File
	mu      sync.Mutex
	reads   int
	maxRead int
}

func (c *countingFile) Read(p []byte) (int, error) {
	n, err := c.File.Read(p)
	c.mu.Lock()
	c.reads++
	if n > c.maxRead {
		c.maxRead = n
	}
	c.mu.Unlock()
	return n, err
}

type hookFs struct {
	Fs
	baseMatch string
	onOpen    func(File) File
}

func (h *hookFs) Open(name string) (File, error) {
	f, err := h.Fs.Open(name)
	if err != nil {
		return nil, err
	}
	if h.onOpen != nil && filepath.Base(name) == h.baseMatch {
		return h.onOpen(f), nil
	}
	return f, nil
}

type errFile struct {
	File
	readErr error
}

func (e *errFile) Read(p []byte) (int, error) {
	n, err := e.File.Read(p)
	// Surface the injected error at end-of-file (or on any read).
	if err == io.EOF {
		return n, e.readErr
	}
	return n, err
}

// ---------- blocking reader (unstable test) ----------

type blockingFile struct {
	File
	once    sync.Once
	started chan struct{}
	proceed chan struct{}
}

func (b *blockingFile) Read(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.started)
		<-b.proceed
	})
	return b.File.Read(p)
}

// ---------- tests ----------

func TestSnapshotBasicDiff(t *testing.T) {
	root := filepath.FromSlash("/tree")
	oldFs := newTestFs(t, root)
	newFs := newTestFs(t, root)

	writeTestFile(t, oldFs, filepath.Join(root, "removed.txt"), []byte("gone"))
	writeTestFile(t, newFs, filepath.Join(root, "added.txt"), []byte("here"))
	writeTestFile(t, oldFs, filepath.Join(root, "same.txt"), []byte("unchanged"))
	writeTestFile(t, newFs, filepath.Join(root, "same.txt"), []byte("unchanged"))
	writeTestFile(t, oldFs, filepath.Join(root, "changed.txt"), []byte("old content"))
	writeTestFile(t, newFs, filepath.Join(root, "changed.txt"), []byte("new content"))

	oldSnap, err := NewSnapshot(oldFs, root)
	if err != nil {
		t.Fatal(err)
	}
	newSnap, err := NewSnapshot(newFs, root)
	if err != nil {
		t.Fatal(err)
	}

	diffs := oldSnap.Diff(newSnap)
	kinds := diffKinds(diffs)

	if kinds["added.txt"] != DiffAdded {
		t.Errorf("added.txt: got %v, want added", kinds["added.txt"])
	}
	if kinds["removed.txt"] != DiffRemoved {
		t.Errorf("removed.txt: got %v, want removed", kinds["removed.txt"])
	}
	if kinds["changed.txt"] != DiffContentChanged {
		t.Errorf("changed.txt: got %v, want content_changed", kinds["changed.txt"])
	}
	if _, ok := kinds["same.txt"]; ok {
		t.Errorf("same.txt should be equal, got %v", kinds["same.txt"])
	}
	if len(diffs) != 3 {
		t.Errorf("got %d diffs, want 3: %v", len(diffs), kinds)
	}
}

func TestSnapshotSameMtimeSameLengthDifferentContent(t *testing.T) {
	root := filepath.FromSlash("/tree")
	oldFs := newTestFs(t, root)
	newFs := newTestFs(t, root)

	writeTestFile(t, oldFs, filepath.Join(root, "f.txt"), []byte("aaaa"))
	writeTestFile(t, newFs, filepath.Join(root, "f.txt"), []byte("bbbb"))

	fixed := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := oldFs.Chtimes(filepath.Join(root, "f.txt"), fixed, fixed); err != nil {
		t.Fatal(err)
	}
	if err := newFs.Chtimes(filepath.Join(root, "f.txt"), fixed, fixed); err != nil {
		t.Fatal(err)
	}

	oldSnap, err := NewSnapshot(oldFs, root)
	if err != nil {
		t.Fatal(err)
	}
	newSnap, err := NewSnapshot(newFs, root)
	if err != nil {
		t.Fatal(err)
	}

	diffs := oldSnap.Diff(newSnap)
	kinds := diffKinds(diffs)
	if kinds["f.txt"] != DiffContentChanged {
		t.Errorf("f.txt: got %v, want content_changed (same mtime+length, different content)", kinds["f.txt"])
	}
}

func TestSnapshotTypeConflict(t *testing.T) {
	root := filepath.FromSlash("/tree")
	oldFs := newTestFs(t, root)
	newFs := newTestFs(t, root)

	writeTestFile(t, oldFs, filepath.Join(root, "name"), []byte("i am a file"))
	mkdirTest(t, newFs, filepath.Join(root, "name"))
	writeTestFile(t, newFs, filepath.Join(root, "name", "child.txt"), []byte("child"))

	oldSnap, err := NewSnapshot(oldFs, root)
	if err != nil {
		t.Fatal(err)
	}
	newSnap, err := NewSnapshot(newFs, root)
	if err != nil {
		t.Fatal(err)
	}

	diffs := oldSnap.Diff(newSnap)
	kinds := diffKinds(diffs)
	if kinds["name"] != DiffTypeConflict {
		t.Errorf("name: got %v, want type_conflict", kinds["name"])
	}
	if kinds[filepath.Join("name", "child.txt")] != DiffAdded {
		t.Errorf("name/child.txt: got %v, want added", kinds[filepath.Join("name", "child.txt")])
	}
}

func TestSnapshotUnstable(t *testing.T) {
	root := filepath.FromSlash("/tree")
	inner := newTestFs(t, root)

	started := make(chan struct{})
	proceed := make(chan struct{})
	fs := &hookFs{
		Fs:        inner,
		baseMatch: "unstable.txt",
		onOpen: func(f File) File {
			return &blockingFile{File: f, started: started, proceed: proceed}
		},
	}

	writeTestFile(t, fs, filepath.Join(root, "unstable.txt"), []byte("original"))

	snapDone := make(chan *Snapshot, 1)
	snapErr := make(chan error, 1)
	go func() {
		s, err := NewSnapshot(fs, root)
		if err != nil {
			snapErr <- err
			return
		}
		snapDone <- s
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for read to start")
	}

	if err := WriteFile(inner, filepath.Join(root, "unstable.txt"), []byte("modified content that is longer"), 0o644); err != nil {
		t.Fatal(err)
	}
	close(proceed)

	var snap *Snapshot
	select {
	case snap = <-snapDone:
	case err := <-snapErr:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for snapshot")
	}

	node := snap.Get("unstable.txt")
	if node == nil {
		t.Fatal("unstable.txt not captured")
	}
	if !node.Unstable {
		t.Errorf("unstable.txt: got Unstable=false, want true (file changed during read)")
	}
}

func TestSnapshotSimilarPrefixDirectories(t *testing.T) {
	root := filepath.FromSlash("/tree")
	oldFs := newTestFs(t, root)
	newFs := newTestFs(t, root)

	mkdirTest(t, oldFs, filepath.Join(root, "foo"))
	mkdirTest(t, oldFs, filepath.Join(root, "foobar"))
	mkdirTest(t, newFs, filepath.Join(root, "foo"))
	mkdirTest(t, newFs, filepath.Join(root, "foobar"))

	writeTestFile(t, oldFs, filepath.Join(root, "foo", "a.txt"), []byte("foo-a"))
	writeTestFile(t, oldFs, filepath.Join(root, "foobar", "b.txt"), []byte("foobar-b"))
	writeTestFile(t, newFs, filepath.Join(root, "foo", "a.txt"), []byte("foo-a"))
	writeTestFile(t, newFs, filepath.Join(root, "foobar", "b.txt"), []byte("foobar-b-CHANGED"))

	oldSnap, err := NewSnapshot(oldFs, root)
	if err != nil {
		t.Fatal(err)
	}
	newSnap, err := NewSnapshot(newFs, root)
	if err != nil {
		t.Fatal(err)
	}

	oldPaths := oldSnap.Paths()
	wantOld := []string{
		"foo",
		filepath.Join("foo", "a.txt"),
		"foobar",
		filepath.Join("foobar", "b.txt"),
	}
	if !equalStrings(oldPaths, wantOld) {
		t.Errorf("old paths: got %v, want %v", oldPaths, wantOld)
	}

	diffs := oldSnap.Diff(newSnap)
	kinds := diffKinds(diffs)
	if kinds[filepath.Join("foobar", "b.txt")] != DiffContentChanged {
		t.Errorf("foobar/b.txt: got %v, want content_changed", kinds[filepath.Join("foobar", "b.txt")])
	}
	if _, ok := kinds[filepath.Join("foo", "a.txt")]; ok {
		t.Errorf("foo/a.txt should be equal, got %v", kinds[filepath.Join("foo", "a.txt")])
	}
	if len(diffs) != 1 {
		t.Errorf("got %d diffs, want 1: %v", len(diffs), kinds)
	}
}

func TestSnapshotBlockSizeAndCountingReader(t *testing.T) {
	root := filepath.FromSlash("/tree")
	inner := newTestFs(t, root)

	const blockSize = 4
	data := []byte("0123456789")

	var counter *countingFile
	fs := &hookFs{
		Fs:        inner,
		baseMatch: "f.bin",
		onOpen: func(f File) File {
			counter = &countingFile{File: f}
			return counter
		},
	}
	writeTestFile(t, fs, filepath.Join(root, "f.bin"), data)

	snap, err := NewSnapshot(fs, root, SnapshotOptions{BlockSize: blockSize})
	if err != nil {
		t.Fatal(err)
	}
	node := snap.Get("f.bin")
	if node == nil {
		t.Fatal("f.bin not captured")
	}

	wantDigest := sha256.Sum256(data)
	if !bytes.Equal(node.Digest, wantDigest[:]) {
		t.Errorf("digest mismatch: got %x, want %x", node.Digest, wantDigest[:])
	}
	counter.mu.Lock()
	gotReads := counter.reads
	gotMaxRead := counter.maxRead
	counter.mu.Unlock()
	if gotMaxRead > blockSize {
		t.Errorf("max read size %d exceeds block size %d", gotMaxRead, blockSize)
	}
	if gotReads == 0 {
		t.Errorf("expected reads > 0, got 0")
	}
}

func TestSnapshotReadError(t *testing.T) {
	root := filepath.FromSlash("/tree")
	inner := newTestFs(t, root)

	readErr := io.ErrUnexpectedEOF
	fs := &hookFs{
		Fs:        inner,
		baseMatch: "bad.bin",
		onOpen: func(f File) File {
			return &errFile{File: f, readErr: readErr}
		},
	}
	writeTestFile(t, fs, filepath.Join(root, "bad.bin"), []byte("some data"))

	snap, err := NewSnapshot(fs, root)
	if err != nil {
		t.Fatal(err)
	}
	node := snap.Get("bad.bin")
	if node == nil {
		t.Fatal("bad.bin not captured")
	}
	if node.ReadErr == nil {
		t.Errorf("expected ReadErr, got nil")
	}
	if node.ReadErr != readErr {
		t.Errorf("ReadErr: got %v, want %v", node.ReadErr, readErr)
	}
}

func TestSnapshotUnsupportedSpecialNode(t *testing.T) {
	oldSnap := &Snapshot{root: "/tree", entries: map[string]*NodeSnapshot{
		"special": {Kind: NodeKindSpecial, Mode: os.ModeDevice},
	}}
	newSnap := &Snapshot{root: "/tree", entries: map[string]*NodeSnapshot{
		"special": {Kind: NodeKindSpecial, Mode: os.ModeDevice},
	}}
	diffs := oldSnap.Diff(newSnap)
	if len(diffs) != 1 {
		t.Fatalf("got %d diffs, want 1", len(diffs))
	}
	if diffs[0].Kind != DiffUnsupported {
		t.Errorf("special: got %v, want unsupported", diffs[0].Kind)
	}
}

func TestSnapshotSymlinkComparison(t *testing.T) {
	tests := []struct {
		name string
		a, b *NodeSnapshot
		want DiffKind
	}{
		{
			name: "equal symlinks",
			a:    &NodeSnapshot{Kind: NodeKindSymlink, Link: "/target"},
			b:    &NodeSnapshot{Kind: NodeKindSymlink, Link: "/target"},
			want: -1,
		},
		{
			name: "changed symlink target",
			a:    &NodeSnapshot{Kind: NodeKindSymlink, Link: "/old"},
			b:    &NodeSnapshot{Kind: NodeKindSymlink, Link: "/new"},
			want: DiffContentChanged,
		},
		{
			name: "file vs symlink type conflict",
			a:    &NodeSnapshot{Kind: NodeKindFile, Digest: []byte("x")},
			b:    &NodeSnapshot{Kind: NodeKindSymlink, Link: "/target"},
			want: DiffTypeConflict,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldSnap := &Snapshot{root: "/tree", entries: map[string]*NodeSnapshot{"p": tc.a}}
			newSnap := &Snapshot{root: "/tree", entries: map[string]*NodeSnapshot{"p": tc.b}}
			diffs := oldSnap.Diff(newSnap)
			if tc.want == -1 {
				if len(diffs) != 0 {
					t.Errorf("got %d diffs, want 0", len(diffs))
				}
				return
			}
			if len(diffs) != 1 {
				t.Fatalf("got %d diffs, want 1", len(diffs))
			}
			if diffs[0].Kind != tc.want {
				t.Errorf("got %v, want %v", diffs[0].Kind, tc.want)
			}
		})
	}
}

func TestSnapshotNeverWrites(t *testing.T) {
	root := filepath.FromSlash("/tree")
	fs := newTestFs(t, root)
	writeTestFile(t, fs, filepath.Join(root, "f.txt"), []byte("content"))

	before, err := fs.Stat(filepath.Join(root, "f.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSnapshot(fs, root); err != nil {
		t.Fatal(err)
	}
	after, err := fs.Stat(filepath.Join(root, "f.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !before.ModTime().Equal(after.ModTime()) {
		t.Errorf("mtime changed during snapshot: %v -> %v", before.ModTime(), after.ModTime())
	}
	if before.Size() != after.Size() {
		t.Errorf("size changed during snapshot: %d -> %d", before.Size(), after.Size())
	}
}

func TestSnapshotCustomHash(t *testing.T) {
	root := filepath.FromSlash("/tree")
	fs := newTestFs(t, root)
	writeTestFile(t, fs, filepath.Join(root, "f.txt"), []byte("hello"))

	snap, err := NewSnapshot(fs, root, SnapshotOptions{
		BlockSize: 2,
		Hash:      func() hash.Hash { return sha256.New() },
	})
	if err != nil {
		t.Fatal(err)
	}
	node := snap.Get("f.txt")
	if node == nil {
		t.Fatal("f.txt not captured")
	}
	want := sha256.Sum256([]byte("hello"))
	if !bytes.Equal(node.Digest, want[:]) {
		t.Errorf("digest mismatch: got %x, want %x", node.Digest, want[:])
	}
}

func TestDiffString(t *testing.T) {
	diffs := []Diff{
		{Path: "a.txt", Kind: DiffAdded, New: &NodeSnapshot{Kind: NodeKindFile}},
		{Path: "b.txt", Kind: DiffRemoved, Old: &NodeSnapshot{Kind: NodeKindFile}},
		{Path: "c.txt", Kind: DiffTypeConflict, Old: &NodeSnapshot{Kind: NodeKindFile}, New: &NodeSnapshot{Kind: NodeKindDir}},
		{Path: "d.txt", Kind: DiffContentChanged, Old: &NodeSnapshot{Kind: NodeKindFile}, New: &NodeSnapshot{Kind: NodeKindFile}},
		{Path: "e.txt", Kind: DiffUnstable, Old: &NodeSnapshot{Kind: NodeKindFile, Unstable: true}},
		{Path: "f.txt", Kind: DiffUnsupported, Old: &NodeSnapshot{Kind: NodeKindSpecial}, New: &NodeSnapshot{Kind: NodeKindSpecial}},
	}
	var buf bytes.Buffer
	PrintDiffs(&buf, diffs)
	out := buf.String()
	for _, d := range diffs {
		if !strings.Contains(out, d.Path) {
			t.Errorf("output missing path %q", d.Path)
		}
		if !strings.Contains(out, d.Kind.String()) {
			t.Errorf("output missing kind %q", d.Kind.String())
		}
	}
	if !strings.Contains(out, "type_conflict") {
		t.Errorf("output missing type_conflict detail")
	}
}
