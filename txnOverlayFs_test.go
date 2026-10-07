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
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero/mem"
)

func fakeTime(unix int64) time.Time { return time.Unix(unix, 0) }

func mustWriteFile(t *testing.T, fs Fs, name, content string) {
	t.Helper()
	if err := fs.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", name, err)
	}
	if err := WriteFile(fs, name, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func readFileStr(t *testing.T, fs Fs, name string) string {
	t.Helper()
	b, err := ReadFile(fs, name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

func expectNotExist(t *testing.T, fs Fs, name string) {
	t.Helper()
	if _, err := fs.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist, got err=%v", name, err)
	}
}

func expectExist(t *testing.T, fs Fs, name string) {
	t.Helper()
	if _, err := fs.Stat(name); err != nil {
		t.Fatalf("expected %s to exist, got err=%v", name, err)
	}
}

func listNames(t *testing.T, fs Fs, dir string) []string {
	t.Helper()
	f, err := fs.Open(dir)
	if err != nil {
		t.Fatalf("open dir %s: %v", dir, err)
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("readdir %s: %v", dir, err)
	}
	sort.Strings(names)
	return names
}

// TestTxnOverlayAcceptance is the headline scenario: modify a, delete b,
// rename directory c to d. Outside the transaction everything looks original
// until a successful commit, after which the complete new view is visible at
// once.
func TestTxnOverlayAcceptance(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A1")
	mustWriteFile(t, base, "/b", "B1")
	mustWriteFile(t, base, "/c/inner", "C1")
	mustWriteFile(t, base, "/c/deep/x", "X1")

	txn := NewTxnOverlayFs(base)

	// In-transaction changes.
	if err := WriteFile(txn, "/a", []byte("A2"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.Remove("/b"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/c", "/d"); err != nil {
		t.Fatal(err)
	}

	// Inside the transaction: new view.
	if got := readFileStr(t, txn, "/a"); got != "A2" {
		t.Fatalf("txn /a = %q, want A2", got)
	}
	expectNotExist(t, txn, "/b")
	expectNotExist(t, txn, "/c")
	expectExist(t, txn, "/d")
	if got := readFileStr(t, txn, "/d/inner"); got != "C1" {
		t.Fatalf("txn /d/inner = %q, want C1", got)
	}
	if got := readFileStr(t, txn, "/d/deep/x"); got != "X1" {
		t.Fatalf("txn /d/deep/x = %q, want X1", got)
	}
	if got := listNames(t, txn, "/"); !equalStrings(got, []string{"a", "d"}) {
		t.Fatalf("txn root listing = %v, want [a d]", got)
	}

	// Outside the transaction (pre-commit): original view is untouched.
	if got := readFileStr(t, base, "/a"); got != "A1" {
		t.Fatalf("base /a before commit = %q, want A1", got)
	}
	expectExist(t, base, "/b")
	expectExist(t, base, "/c")
	expectNotExist(t, base, "/d")
	if got := readFileStr(t, base, "/c/inner"); got != "C1" {
		t.Fatalf("base /c/inner = %q, want C1", got)
	}
	if got := listNames(t, base, "/"); !equalStrings(got, []string{"a", "b", "c"}) {
		t.Fatalf("base root listing before commit = %v, want [a b c]", got)
	}

	// Commit: whole new view becomes visible atomically.
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readFileStr(t, base, "/a"); got != "A2" {
		t.Fatalf("base /a after commit = %q, want A2", got)
	}
	expectNotExist(t, base, "/b")
	expectNotExist(t, base, "/c")
	expectExist(t, base, "/d")
	if got := readFileStr(t, base, "/d/inner"); got != "C1" {
		t.Fatalf("base /d/inner = %q, want C1", got)
	}
	if got := readFileStr(t, base, "/d/deep/x"); got != "X1" {
		t.Fatalf("base /d/deep/x = %q, want X1", got)
	}
	if got := listNames(t, base, "/"); !equalStrings(got, []string{"a", "d"}) {
		t.Fatalf("base root listing after commit = %v, want [a d]", got)
	}
	if got := listNames(t, base, "/d"); !equalStrings(got, []string{"deep", "inner"}) {
		t.Fatalf("base /d listing = %v, want [deep inner]", got)
	}
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

// TestTxnConflictAndRollback verifies that a base change to a path the
// transaction read or modified causes the whole commit to be rejected, that
// nothing is applied, and that after rollback both sides agree.
func TestTxnConflictAndRollback(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A1")
	mustWriteFile(t, base, "/b", "B1")

	txn := NewTxnOverlayFs(base)
	// Touch a (record its initial version) and modify it in the txn.
	if got := readFileStr(t, txn, "/a"); got != "A1" {
		t.Fatalf("txn /a = %q", got)
	}
	if err := WriteFile(txn, "/a", []byte("A2"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Concurrent modification to a through the base.
	if err := WriteFile(base, "/a", []byte("A-concurrent"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := txn.Commit()
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	var ce *TxnConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *TxnConflictError, got %T: %v", err, err)
	}
	if ce.Path != normalizePath("/a") {
		t.Fatalf("conflict path = %q, want %q", ce.Path, normalizePath("/a"))
	}

	// Nothing was applied.
	if got := readFileStr(t, base, "/a"); got != "A-concurrent" {
		t.Fatalf("base /a after failed commit = %q", got)
	}

	// Roll back and make sure a fresh transaction's view equals the base view.
	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	txn2 := NewTxnOverlayFs(base)
	for _, p := range []string{"/a", "/b"} {
		if readFileStr(t, txn2, p) != readFileStr(t, base, p) {
			t.Fatalf("views diverge at %s", p)
		}
	}
	expectNotExist(t, txn2, "/c")
}

// TestTxnOpenFileHandle: handles opened in the transaction behave correctly;
// in particular a stale writable handle must not leak into the base after the
// transaction ends.
func TestTxnOpenFileHandle(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A1")

	txn := NewTxnOverlayFs(base)

	rf, err := txn.Open("/a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(rf)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "A1" {
		t.Fatalf("read-only handle got %q", b)
	}
	if err := rf.Close(); err != nil {
		t.Fatal(err)
	}

	// Writable handle copied up into the layer.
	wf, err := txn.OpenFile("/a", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.WriteString("A2"); err != nil {
		t.Fatal(err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	// Base untouched pre-commit.
	if got := readFileStr(t, base, "/a"); got != "A1" {
		t.Fatalf("base changed before commit: %q", got)
	}

	// Base concurrently changes a -> the txn must conflict.
	if err := WriteFile(base, "/a", []byte("A3"), 0o644); err != nil {
		t.Fatal(err)
	}
	err = txn.Commit()
	var ce *TxnConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}

	// The stale handle must not be able to mutate the (new) base.
	if _, err := wf.WriteString("boom"); err == nil {
		t.Fatal("expected error writing through a rolled-back handle")
	}
	if got := readFileStr(t, base, "/a"); got != "A3" {
		t.Fatalf("base corrupted by stale handle: %q", got)
	}
}

// TestTxnRenameLoop exercises a sequence of cross-directory renames that move
// things in a cycle; base children must travel along and parents must end up
// consistent.
func TestTxnRenameLoop(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/p/x/f", "FX")
	mustWriteFile(t, base, "/q/y/g", "GY")

	txn := NewTxnOverlayFs(base)
	if err := txn.Rename("/p/x", "/q/x"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/q/y", "/p/y"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/q/x", "/p/x"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/p/y", "/q/y"); err != nil {
		t.Fatal(err)
	}

	// Contents must still be reachable.
	if got := readFileStr(t, txn, "/p/x/f"); got != "FX" {
		t.Fatalf("loop /p/x/f = %q", got)
	}
	if got := readFileStr(t, txn, "/q/y/g"); got != "GY" {
		t.Fatalf("loop /q/y/g = %q", got)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readFileStr(t, base, "/p/x/f"); got != "FX" {
		t.Fatalf("post-commit /p/x/f = %q", got)
	}
	if got := readFileStr(t, base, "/q/y/g"); got != "GY" {
		t.Fatalf("post-commit /q/y/g = %q", got)
	}
	if got := listNames(t, base, "/p"); !equalStrings(got, []string{"x"}) {
		t.Fatalf("/p = %v", got)
	}
	if got := listNames(t, base, "/q"); !equalStrings(got, []string{"y"}) {
		t.Fatalf("/q = %v", got)
	}

	// Another loop form: rename over an existing base file.
	base2 := NewTxnMemMapFs()
	mustWriteFile(t, base2, "/src", "S")
	mustWriteFile(t, base2, "/dst", "D")
	t2 := NewTxnOverlayFs(base2)
	if err := t2.Rename("/src", "/dst"); err != nil {
		t.Fatal(err)
	}
	if got := readFileStr(t, t2, "/dst"); got != "S" {
		t.Fatalf("overwritten /dst = %q", got)
	}
	expectNotExist(t, t2, "/src")
	if err := t2.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readFileStr(t, base2, "/dst"); got != "S" {
		t.Fatalf("post-commit /dst = %q", got)
	}
	expectNotExist(t, base2, "/src")
}

// TestTxnDuplicateCommit: a second Commit and post-terminal operations are
// rejected.
func TestTxnDuplicateCommit(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A1")
	txn := NewTxnOverlayFs(base)
	if err := WriteFile(txn, "/a", []byte("A2"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if err := txn.Commit(); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("second commit err = %v, want ErrTxnClosed", err)
	}
	if err := txn.Rollback(); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("rollback after commit err = %v, want ErrTxnClosed", err)
	}
	if _, err := txn.Stat("/a"); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("stat after commit err = %v, want ErrTxnClosed", err)
	}
	if err := txn.Remove("/a"); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("remove after commit err = %v, want ErrTxnClosed", err)
	}

	// Rollback terminal as well.
	base2 := NewTxnMemMapFs()
	t2 := NewTxnOverlayFs(base2)
	if err := t2.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("commit after rollback err = %v", err)
	}
}

// TestTxnDirectoryWhiteout: removing a directory in the txn hides the whole
// base subtree; recreating a same-named directory in the txn must not let base
// children show through. Deleted directories become whiteouts published on
// commit.
func TestTxnDirectoryWhiteout(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/c/f1", "1")
	mustWriteFile(t, base, "/c/sub/f2", "2")
	mustWriteFile(t, base, "/other", "O")

	txn := NewTxnOverlayFs(base)
	if err := txn.RemoveAll("/c"); err != nil {
		t.Fatal(err)
	}
	expectNotExist(t, txn, "/c")
	expectNotExist(t, txn, "/c/f1")
	expectNotExist(t, txn, "/c/sub/f2")
	if got := listNames(t, txn, "/"); !equalStrings(got, []string{"other"}) {
		t.Fatalf("root listing = %v, want [other]", got)
	}

	// Recreate /c in the txn: it must be opaque, base children stay hidden.
	if err := txn.Mkdir("/c", 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, txn, "/c/new", "N")
	if got := listNames(t, txn, "/c"); !equalStrings(got, []string{"new"}) {
		t.Fatalf("/c = %v, want [new]", got)
	}
	expectNotExist(t, txn, "/c/f1")

	// Base unchanged.
	expectExist(t, base, "/c/f1")
	expectExist(t, base, "/c/sub/f2")

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	expectNotExist(t, base, "/c/f1")
	expectNotExist(t, base, "/c/sub/f2")
	expectNotExist(t, base, "/c/sub")
	if got := readFileStr(t, base, "/c/new"); got != "N" {
		t.Fatalf("/c/new = %q", got)
	}
	if got := listNames(t, base, "/c"); !equalStrings(got, []string{"new"}) {
		t.Fatalf("post /c = %v, want [new]", got)
	}

	// Empty-dir Remove requires the directory to be empty.
	base2 := NewTxnMemMapFs()
	mustWriteFile(t, base2, "/d/child", "x")
	t2 := NewTxnOverlayFs(base2)
	if err := t2.Remove("/d"); err == nil {
		t.Fatal("Remove on non-empty dir should fail")
	}
	if err := t2.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestTxnConflictUnderRemovedDir: a file added to a removed directory via the
// base between removal and commit must cause a conflict.
func TestTxnConflictUnderRemovedDir(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/c/a", "A")
	txn := NewTxnOverlayFs(base)
	if err := txn.RemoveAll("/c"); err != nil {
		t.Fatal(err)
	}
	// Someone adds a new child directly in the base.
	mustWriteFile(t, base, "/c/b", "B")
	err := txn.Commit()
	var ce *TxnConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected conflict, got %v", err)
	}
	// Base untouched after rejected commit.
	expectExist(t, base, "/c/a")
	expectExist(t, base, "/c/b")
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	fresh := NewTxnOverlayFs(base)
	expectExist(t, fresh, "/c/a")
	expectExist(t, fresh, "/c/b")
}

// TestTxnRollbackLeavesBaseIntact and view agreement on both sides.
func TestTxnRollbackLeavesBaseIntact(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A")
	mustWriteFile(t, base, "/c/f", "F")
	txn := NewTxnOverlayFs(base)
	if err := WriteFile(txn, "/a", []byte("X"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.RemoveAll("/c"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Mkdir("/newdir", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	fresh := NewTxnOverlayFs(base)
	if got := readFileStr(t, fresh, "/a"); got != "A" {
		t.Fatalf("fresh /a = %q", got)
	}
	if got := readFileStr(t, fresh, "/c/f"); got != "F" {
		t.Fatalf("fresh /c/f = %q", got)
	}
	expectNotExist(t, fresh, "/newdir")
}

// TestTxnCrossDirRenameParents: cross-directory renames update both parents'
// directory entries as part of the atomic commit.
func TestTxnCrossDirRenameParents(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/dir1/f", "F")
	txn := NewTxnOverlayFs(base)
	if err := txn.MkdirAll("/dir2", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/dir1/f", "/dir2/f"); err != nil {
		t.Fatal(err)
	}
	if got := listNames(t, txn, "/dir1"); len(got) != 0 {
		t.Fatalf("txn /dir1 = %v, want empty", got)
	}
	if got := listNames(t, txn, "/dir2"); !equalStrings(got, []string{"f"}) {
		t.Fatalf("txn /dir2 = %v", got)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := listNames(t, base, "/dir1"); len(got) != 0 {
		t.Fatalf("base /dir1 = %v, want empty", got)
	}
	if got := listNames(t, base, "/dir2"); !equalStrings(got, []string{"f"}) {
		t.Fatalf("base /dir2 = %v", got)
	}
	if got := readFileStr(t, base, "/dir2/f"); got != "F" {
		t.Fatalf("/dir2/f = %q", got)
	}
}

// TestTxnCreateNewFilesAndDirs covers purely additive transactions.
func TestTxnCreateNewFilesAndDirs(t *testing.T) {
	base := NewTxnMemMapFs()
	txn := NewTxnOverlayFs(base)
	mustWriteFile(t, txn, "/x/y/z", "Z")
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readFileStr(t, base, "/x/y/z"); got != "Z" {
		t.Fatalf("/x/y/z = %q", got)
	}
	fi, err := base.Stat("/x")
	if err != nil || !fi.IsDir() {
		t.Fatalf("/x should be a directory: %v", err)
	}
}

// TestTxnMergedReads: reads see merged layer+base content.
func TestTxnMergedReads(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/d/base-only", "B")
	mustWriteFile(t, base, "/d/shared", "base")
	txn := NewTxnOverlayFs(base)
	mustWriteFile(t, txn, "/d/txn-only", "T")
	mustWriteFile(t, txn, "/d/shared", "txn")
	got := listNames(t, txn, "/d")
	if !equalStrings(got, []string{"base-only", "shared", "txn-only"}) {
		t.Fatalf("merged listing = %v", got)
	}
	if s := readFileStr(t, txn, "/d/shared"); s != "txn" {
		t.Fatalf("shared = %q, want txn", s)
	}
	if s := readFileStr(t, txn, "/d/base-only"); s != "B" {
		t.Fatalf("base-only = %q", s)
	}
}

// TestTxnEmptyCommit: committing a transaction that changed nothing is a
// no-op success (and remains one-shot).
func TestTxnEmptyCommit(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A")
	txn := NewTxnOverlayFs(base)
	_ = readFileStr(t, txn, "/a")
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readFileStr(t, base, "/a"); got != "A" {
		t.Fatalf("/a = %q", got)
	}
}

// TestTxnConcurrentRenameConflict: a base rename of a file the txn reads is a
// conflict.
func TestTxnConcurrentRenameConflict(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A")
	txn := NewTxnOverlayFs(base)
	_ = readFileStr(t, txn, "/a")
	if err := base.Rename("/a", "/b"); err != nil {
		t.Fatal(err)
	}
	err := txn.Commit()
	var ce *TxnConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

// TestTxnMemMapFsBumpsVersions ensures direct base mutations bump versions
// and EntryVersion reports them.
func TestTxnMemMapFsBumpsVersions(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A")
	v1, ok := base.EntryVersion("/a")
	if !ok {
		t.Fatal("/a missing")
	}
	if err := WriteFile(base, "/a", []byte("AA"), 0o644); err != nil {
		t.Fatal(err)
	}
	v2, _ := base.EntryVersion("/a")
	if v2 == v1 {
		t.Fatalf("version not bumped after write: %d", v1)
	}

	// A handle write bumps the version.
	f, err := base.OpenFile("/a", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("AAA"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	v3, _ := base.EntryVersion("/a")
	if v3 == v2 {
		t.Fatal("version not bumped after handle write")
	}

	// Parent version bumps when directory membership changes.
	pr1, _ := base.EntryVersion("/")
	mustWriteFile(t, base, "/new", "N")
	pr2, _ := base.EntryVersion("/")
	if pr2 == pr1 {
		t.Fatal("root version not bumped after adding a child")
	}
}

// TestTxnChmodChownChtimesCopyUp verifies metadata ops are private and
// committed via the layer.
func TestTxnChmodChownChtimesCopyUp(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "A")
	orig, _ := base.Stat("/a")
	txn := NewTxnOverlayFs(base)
	if err := txn.Chmod("/a", 0o600); err != nil {
		t.Fatal(err)
	}
	fi, err := txn.Stat("/a")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("txn mode = %o", fi.Mode().Perm())
	}
	if bfi, _ := base.Stat("/a"); bfi.Mode().Perm() != orig.Mode().Perm() {
		t.Fatal("base mode changed before commit")
	}
	if err := txn.Chtimes("/a", fakeTime(1), fakeTime(2)); err != nil {
		t.Fatal(err)
	}
	if err := txn.Chown("/a", 7, 9); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	fi, _ = base.Stat("/a")
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("committed mode = %o", fi.Mode().Perm())
	}
}

// TestTxnRenameIntoSelfRejected.
func TestTxnRenameIntoSelfRejected(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/d/f", "F")
	txn := NewTxnOverlayFs(base)
	if err := txn.MkdirAll("/d/sub", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/d", "/d/sub/x"); err == nil {
		t.Fatal("expected error renaming dir into its own descendant")
	}
}

// TestTxnRenameLoopNested is a more demanding rename cycle involving moves
// with additional base children and intermediate edits.
func TestTxnRenameLoopNested(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a/x/1", "one")
	mustWriteFile(t, base, "/a/x/2", "two")
	mustWriteFile(t, base, "/b/y/3", "three")

	txn := NewTxnOverlayFs(base)
	if err := txn.Rename("/a/x", "/b/x"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/b/y", "/a/y"); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(txn, "/b/x/1", []byte("ONE"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/b/x", "/a/x"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/a/y", "/b/y"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readFileStr(t, base, "/a/x/1"); got != "ONE" {
		t.Fatalf("/a/x/1 = %q", got)
	}
	if got := readFileStr(t, base, "/a/x/2"); got != "two" {
		t.Fatalf("/a/x/2 = %q", got)
	}
	if got := readFileStr(t, base, "/b/y/3"); got != "three" {
		t.Fatalf("/b/y/3 = %q", got)
	}
}

// TestTxnAtomicBatchNoPartialView checks that while a successful commit is in
// progress an observer either sees the old or the complete new state. (Under
// the single-write-lock batch protocol this is guaranteed structurally; we
// at least verify the final state is consistent.)
func TestTxnAtomicBatchNoPartialView(t *testing.T) {
	base := NewTxnMemMapFs()
	for _, p := range []string{"/d1/a", "/d1/b", "/d2/c"} {
		mustWriteFile(t, base, p, "v")
	}
	txn := NewTxnOverlayFs(base)
	if err := txn.MkdirAll("/d3", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, op := range []func() error{
		func() error { return txn.Rename("/d1/a", "/d3/a") },
		func() error { return txn.Rename("/d1/b", "/d3/b") },
		func() error { return txn.RemoveAll("/d2") },
	} {
		if err := op(); err != nil {
			t.Fatal(err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := listNames(t, base, "/"); !equalStrings(got, []string{"d1", "d3"}) {
		t.Fatalf("root = %v, want [d1 d3]", got)
	}
	if got := listNames(t, base, "/d1"); len(got) != 0 {
		t.Fatalf("/d1 = %v, want empty", got)
	}
	if got := listNames(t, base, "/d3"); !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("/d3 = %v, want [a b]", got)
	}
	expectNotExist(t, base, "/d2")
	expectNotExist(t, base, "/d2/c")
}

// TestTxnCreateOverWhiteoutFile recreates a deleted file in the same
// transaction and then commits.
func TestTxnCreateOverWhiteoutFile(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/f", "old")
	txn := NewTxnOverlayFs(base)
	if err := txn.Remove("/f"); err != nil {
		t.Fatal(err)
	}
	expectNotExist(t, txn, "/f")
	mustWriteFile(t, txn, "/f", "new")
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readFileStr(t, base, "/f"); got != "new" {
		t.Fatalf("/f = %q, want new", got)
	}
}

// TestTxnConcurrentBaseChangeUnderRenamedDir.
func TestTxnConcurrentBaseChangeUnderRenamedDir(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/c/a", "A")
	txn := NewTxnOverlayFs(base)
	if err := txn.Rename("/c", "/d"); err != nil {
		t.Fatal(err)
	}
	// Base adds a file under /c concurrently.
	mustWriteFile(t, base, "/c/b", "B")
	err := txn.Commit()
	var ce *TxnConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected conflict, got %v", err)
	}
	// Base intact.
	if got := readFileStr(t, base, "/c/a"); got != "A" {
		t.Fatalf("/c/a = %q", got)
	}
	if got := readFileStr(t, base, "/c/b"); got != "B" {
		t.Fatalf("/c/b = %q", got)
	}
}

// TestTxnMergedDirCommitsLiveChildren verifies that after committing changes
// to one child of a base directory (while untouched base siblings remain),
// the published directory listing references the actual published nodes with
// correct content (regression test for stale layer pointers in the union).
func TestTxnMergedDirCommitsLiveChildren(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/d/base1", "B1")
	mustWriteFile(t, base, "/d/base2", "B2")
	mustWriteFile(t, base, "/d/edited", "OLD")

	txn := NewTxnOverlayFs(base)
	if err := WriteFile(txn, "/d/edited", []byte("NEW"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, txn, "/d/new1", "N1")
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Directory listing content must resolve correctly via Open + Stat too.
	f, err := base.Open("/d")
	if err != nil {
		t.Fatal(err)
	}
	fis, err := f.Readdir(-1)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(fis))
	for _, fi := range fis {
		names = append(names, fi.Name())
	}
	sort.Strings(names)
	if !equalStrings(names, []string{"base1", "base2", "edited", "new1"}) {
		t.Fatalf("/d = %v", names)
	}
	// Confirm every child's content is readable (a stale layer FileData would
	// have been consumed/closed or hold wrong bytes).
	want := map[string]string{
		"/d/base1":  "B1",
		"/d/base2":  "B2",
		"/d/edited": "NEW",
		"/d/new1":   "N1",
	}
	for p, w := range want {
		if got := readFileStr(t, base, p); got != w {
			t.Fatalf("%s = %q, want %q", p, got, w)
		}
	}
}

// TestTxnFileUnderRecreatedWhiteoutDir creates a file in a fresh subdirectory
// beneath a removed-and-recreated directory; the commit must recreate rather
// than merge intermediate directories.
func TestTxnFileUnderRecreatedWhiteoutDir(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/c/oldsub/old", "O")
	txn := NewTxnOverlayFs(base)
	if err := txn.RemoveAll("/c"); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, txn, "/c/newsub/new", "N")
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	expectNotExist(t, base, "/c/oldsub")
	expectNotExist(t, base, "/c/oldsub/old")
	if got := readFileStr(t, base, "/c/newsub/new"); got != "N" {
		t.Fatalf("/c/newsub/new = %q", got)
	}
	if got := listNames(t, base, "/c"); !equalStrings(got, []string{"newsub"}) {
		t.Fatalf("/c = %v, want [newsub]", got)
	}
}

// TestTxnSameParentRename renames inside one directory; the shared parent
// listing must end up correct after the atomic commit.
func TestTxnSameParentRename(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/dir/a", "A")
	mustWriteFile(t, base, "/dir/b", "B")

	txn := NewTxnOverlayFs(base)
	if err := txn.Rename("/dir/a", "/dir/c"); err != nil {
		t.Fatal(err)
	}
	if got := listNames(t, txn, "/dir"); !equalStrings(got, []string{"b", "c"}) {
		t.Fatalf("txn /dir = %v, want [b c]", got)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := listNames(t, base, "/dir"); !equalStrings(got, []string{"b", "c"}) {
		t.Fatalf("base /dir = %v, want [b c]", got)
	}
	expectNotExist(t, base, "/dir/a")
	if got := readFileStr(t, base, "/dir/c"); got != "A" {
		t.Fatalf("/dir/c = %q", got)
	}
}

// TestTxnRenameTxnFileOverBaseFile moves a newly created file onto an existing
// base file path.
func TestTxnRenameTxnFileOverBaseFile(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/dst", "OLD-DST")
	txn := NewTxnOverlayFs(base)
	mustWriteFile(t, txn, "/src", "NEW-SRC")
	if err := txn.Rename("/src", "/dst"); err != nil {
		t.Fatal(err)
	}
	if got := readFileStr(t, txn, "/dst"); got != "NEW-SRC" {
		t.Fatalf("txn /dst = %q", got)
	}
	expectNotExist(t, txn, "/src")
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := readFileStr(t, base, "/dst"); got != "NEW-SRC" {
		t.Fatalf("base /dst = %q", got)
	}
	expectNotExist(t, base, "/src")
	if got := listNames(t, base, "/"); !equalStrings(got, []string{"dst"}) {
		t.Fatalf("root = %v, want [dst]", got)
	}
}

// TestTxnFileAncestorIsNotDir: creating/renaming under a path that is a base
// file must fail rather than silently shadowing it with a directory.
func TestTxnFileAncestorIsNotDir(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/x", "X")
	txn := NewTxnOverlayFs(base)
	if _, err := txn.OpenFile("/x/y", os.O_CREATE|os.O_RDWR, 0o644); !os.IsNotExist(err) {
		t.Fatalf("create under file ancestor err = %v, want not-exist", err)
	}
	if err := txn.Mkdir("/x/y", 0o755); !os.IsNotExist(err) {
		t.Fatalf("mkdir under file ancestor err = %v, want not-exist", err)
	}
	mustWriteFile(t, txn, "/tmp", "T")
	if err := txn.Rename("/tmp", "/x/z"); !os.IsNotExist(err) {
		t.Fatalf("rename under file ancestor err = %v, want not-exist", err)
	}
	fi, _ := txn.Stat("/x")
	if fi.IsDir() {
		t.Fatal("/x must remain a file in the merged view")
	}
}

// snapshotView reads a complete, internally consistent view of the relevant
// paths while holding the base read lock once, which is what the atomic
// batch guarantees (no torn tree within one critical section).
func snapshotView(base *TxnMemMapFs) string {
	base.mu.RLock()
	defer base.mu.RUnlock()
	read := func(p string) string {
		if e, ok := base.getData()[normalizePath(p)]; ok {
			fi := mem.GetFileInfo(e.data)
			if !fi.IsDir() {
				return string(memReadAll(e.data))
			}
			return "DIR"
		}
		return "-"
	}
	exists := func(p string) string {
		if _, ok := base.getData()[normalizePath(p)]; ok {
			return "true"
		}
		return "false"
	}
	return read("/a") + "|" + read("/b") + "|c/f=" + exists("/c/f") + "|d/f=" + exists("/d/f")
}

func memReadAll(fd *mem.FileData) []byte {
	h := mem.NewReadOnlyFileHandle(fd)
	defer h.Close()
	b, _ := io.ReadAll(h)
	return b
}

// TestTxnAtomicVisibility runs concurrent observers taking one locked
// snapshot each against the base while a transaction commits; every snapshot
// must be either fully old or fully new (never a partial mix).
func TestTxnAtomicVisibility(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a", "old")
	mustWriteFile(t, base, "/b", "old")
	mustWriteFile(t, base, "/c/f", "old")

	txn := NewTxnOverlayFs(base)
	if err := WriteFile(txn, "/a", []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(txn, "/b", []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/c", "/d"); err != nil {
		t.Fatal(err)
	}

	oldView := "old|old|c/f=true|d/f=false"
	newView := "new|new|c/f=false|d/f=true"

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var bad int
	var mu sync.Mutex
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v := snapshotView(base)
				mu.Lock()
				if v != oldView && v != newView {
					bad++
				}
				mu.Unlock()
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	if bad != 0 {
		t.Fatalf("observed %d partial (non-atomic) snapshots", bad)
	}
	if got := snapshotView(base); got != newView {
		t.Fatalf("final view = %q, want %q", got, newView)
	}
}

// TestTxnMemMapFsRenameBasics sanity-checks the versioned base on its own.
func TestTxnMemMapFsRenameBasics(t *testing.T) {
	base := NewTxnMemMapFs()
	mustWriteFile(t, base, "/a/f", "F")
	if err := base.Rename("/a", "/b"); err != nil {
		t.Fatal(err)
	}
	expectNotExist(t, base, "/a/f")
	if got := readFileStr(t, base, "/b/f"); got != "F" {
		t.Fatalf("/b/f = %q", got)
	}
	if got := listNames(t, base, "/"); !equalStrings(got, []string{"b"}) {
		t.Fatalf("root = %v", got)
	}
}
