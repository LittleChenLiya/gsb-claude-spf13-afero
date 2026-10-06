package afero

import (
	"errors"
	"io"
	"os"
	"sort"
	"testing"
)

func setupTxnBase(t *testing.T) Fs {
	t.Helper()
	base := NewMemMapFs()
	if err := WriteFile(base, "/a", []byte("A"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(base, "/b", []byte("B"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := base.MkdirAll("/c", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(base, "/c/f", []byte("F"), 0o644); err != nil {
		t.Fatal(err)
	}
	return base
}

func readString(t *testing.T, fs Fs, name string) string {
	t.Helper()
	data, err := ReadFile(fs, name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

func mustNotExist(t *testing.T, fs Fs, name string) {
	t.Helper()
	if _, err := fs.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist, stat err=%v", name, err)
	}
}

func mustExist(t *testing.T, fs Fs, name string) {
	t.Helper()
	if _, err := fs.Stat(name); err != nil {
		t.Fatalf("expected %s to exist: %v", name, err)
	}
}

func dirNames(t *testing.T, fs Fs, name string) []string {
	t.Helper()
	f, err := fs.Open(name)
	if err != nil {
		t.Fatalf("open dir %s: %v", name, err)
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		t.Fatalf("readdirnames %s: %v", name, err)
	}
	sort.Strings(names)
	return names
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

// The core acceptance scenario: inside a transaction modify "a", delete "b"
// and rename directory "c" to "d". The base must observe the old state until
// the commit, and the new state in one piece afterwards.
func TestTxnOverlayCommitPublishesAtomically(t *testing.T) {
	base := setupTxnBase(t)
	txn := NewTxnOverlayFs(base)

	// Stage changes: modify a, delete b, rename dir c -> d.
	if err := WriteFile(txn, "/a", []byte("A2"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.Remove("/b"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rename("/c", "/d"); err != nil {
		t.Fatal(err)
	}

	// Transaction view reflects the changes.
	if got := readString(t, txn, "/a"); got != "A2" {
		t.Fatalf("txn view of /a = %q, want A2", got)
	}
	mustNotExist(t, txn, "/b")
	mustNotExist(t, txn, "/c")
	mustNotExist(t, txn, "/c/f")
	if got := readString(t, txn, "/d/f"); got != "F" {
		t.Fatalf("txn view of /d/f = %q, want F", got)
	}
	if names := dirNames(t, txn, "/"); !equalStrings(names, []string{"a", "d"}) {
		t.Fatalf("txn root listing = %v, want [a d]", names)
	}
	if names := dirNames(t, txn, "/d"); !equalStrings(names, []string{"f"}) {
		t.Fatalf("txn /d listing = %v, want [f]", names)
	}

	// The base still observes the original state before the commit.
	if got := readString(t, base, "/a"); got != "A" {
		t.Fatalf("base view of /a before commit = %q, want A", got)
	}
	mustExist(t, base, "/b")
	mustExist(t, base, "/c")
	if got := readString(t, base, "/c/f"); got != "F" {
		t.Fatalf("base view of /c/f before commit = %q, want F", got)
	}
	mustNotExist(t, base, "/d")
	if names := dirNames(t, base, "/"); !equalStrings(names, []string{"a", "b", "c"}) {
		t.Fatalf("base root listing before commit = %v, want [a b c]", names)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// After the commit the base observes the new view in one piece.
	if got := readString(t, base, "/a"); got != "A2" {
		t.Fatalf("base view of /a after commit = %q, want A2", got)
	}
	mustNotExist(t, base, "/b")
	mustNotExist(t, base, "/c")
	mustNotExist(t, base, "/c/f")
	if got := readString(t, base, "/d/f"); got != "F" {
		t.Fatalf("base view of /d/f after commit = %q, want F", got)
	}
	if names := dirNames(t, base, "/"); !equalStrings(names, []string{"a", "d"}) {
		t.Fatalf("base root listing after commit = %v, want [a d]", names)
	}
}

// A concurrent base modification of a path the transaction touched must
// reject the whole commit; after rolling back, both views agree again.
func TestTxnOverlayConflictAndRollback(t *testing.T) {
	base := setupTxnBase(t)
	txn := NewTxnOverlayFs(base)

	// The transaction reads and modifies /a, deletes /b.
	if got := readString(t, txn, "/a"); got != "A" {
		t.Fatalf("txn view of /a = %q, want A", got)
	}
	if err := WriteFile(txn, "/a", []byte("A2"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.Remove("/b"); err != nil {
		t.Fatal(err)
	}

	// Concurrent base modification of /a.
	if err := WriteFile(base, "/a", []byte("BASE"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := txn.Commit()
	if !errors.Is(err, ErrTxnConflict) {
		t.Fatalf("commit err = %v, want ErrTxnConflict", err)
	}
	var cerr *TxnConflictError
	if !errors.As(err, &cerr) {
		t.Fatalf("commit error is not a *TxnConflictError: %T", err)
	}

	// The failed commit left the base untouched by the transaction.
	if got := readString(t, base, "/a"); got != "BASE" {
		t.Fatalf("base view of /a after failed commit = %q, want BASE", got)
	}
	mustExist(t, base, "/b")

	// Roll back; afterwards both views must agree again.
	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got := readString(t, txn, "/a"); got != readString(t, base, "/a") {
		t.Fatalf("views differ after rollback: txn=%q base=%q",
			readString(t, txn, "/a"), readString(t, base, "/a"))
	}
	if got := readString(t, txn, "/b"); got != "B" {
		t.Fatalf("txn view of /b after rollback = %q, want B", got)
	}
	if got := readString(t, txn, "/c/f"); got != "F" {
		t.Fatalf("txn view of /c/f after rollback = %q, want F", got)
	}
	if names := dirNames(t, txn, "/"); !equalStrings(names, dirNames(t, base, "/")) {
		t.Fatalf("root listings differ after rollback: txn=%v base=%v",
			dirNames(t, txn, "/"), dirNames(t, base, "/"))
	}
}

// A structural change in the base (here: a new file inside a directory the
// transaction renamed away) must also be detected as a conflict.
func TestTxnOverlayRenameParentConflict(t *testing.T) {
	base := setupTxnBase(t)
	txn := NewTxnOverlayFs(base)

	if err := txn.Rename("/c", "/d"); err != nil {
		t.Fatal(err)
	}
	// Concurrent base change inside the renamed-away directory.
	if err := WriteFile(base, "/c/g", []byte("G"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := txn.Commit(); !errors.Is(err, ErrTxnConflict) {
		t.Fatalf("commit err = %v, want ErrTxnConflict", err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	// Both views agree on the (unchanged by the txn) base state.
	mustExist(t, txn, "/c/g")
	mustNotExist(t, txn, "/d")
}

// Handles opened before the commit keep observing the old content; new
// opens observe the committed content.
func TestTxnOverlayOpenHandles(t *testing.T) {
	base := setupTxnBase(t)
	txn := NewTxnOverlayFs(base)

	// Handle opened directly on the base before any transaction activity.
	baseHandle, err := base.Open("/a")
	if err != nil {
		t.Fatal(err)
	}
	defer baseHandle.Close()

	// Handle opened through the transaction before the modification; it
	// refers to the base content.
	txnHandle, err := txn.Open("/a")
	if err != nil {
		t.Fatal(err)
	}
	defer txnHandle.Close()

	if err := WriteFile(txn, "/a", []byte("A2"), 0o644); err != nil {
		t.Fatal(err)
	}

	// A handle opened inside the transaction after the modification sees
	// the staged content.
	inTxn, err := txn.Open("/a")
	if err != nil {
		t.Fatal(err)
	}
	staged, err := io.ReadAll(inTxn)
	inTxn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(staged) != "A2" {
		t.Fatalf("staged handle content = %q, want A2", staged)
	}

	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// The pre-commit handles still observe the old content.
	old, err := io.ReadAll(baseHandle)
	if err != nil {
		t.Fatal(err)
	}
	if string(old) != "A" {
		t.Fatalf("base handle content after commit = %q, want A", string(old))
	}
	oldTxn, err := io.ReadAll(txnHandle)
	if err != nil {
		t.Fatal(err)
	}
	if string(oldTxn) != "A" {
		t.Fatalf("txn handle content after commit = %q, want A", string(oldTxn))
	}

	// New opens observe the committed content.
	if got := readString(t, base, "/a"); got != "A2" {
		t.Fatalf("new base read of /a = %q, want A2", got)
	}
}

// A rename ring (rotation through a temporary name) must be replayed in
// order and leave no leftovers behind.
func TestTxnOverlayRenameRing(t *testing.T) {
	base := NewMemMapFs()
	for _, f := range []struct{ name, content string }{
		{"/a", "A"}, {"/b", "B"}, {"/c", "C"},
	} {
		if err := WriteFile(base, f.name, []byte(f.content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	txn := NewTxnOverlayFs(base)

	// Rotate: a -> tmp, b -> a, c -> b, tmp -> c.
	for _, r := range [][2]string{
		{"/a", "/tmp"}, {"/b", "/a"}, {"/c", "/b"}, {"/tmp", "/c"},
	} {
		if err := txn.Rename(r[0], r[1]); err != nil {
			t.Fatalf("rename %s -> %s: %v", r[0], r[1], err)
		}
	}

	// Transaction view: rotated, no leftover tmp.
	if got := readString(t, txn, "/a"); got != "B" {
		t.Fatalf("txn /a = %q, want B", got)
	}
	if got := readString(t, txn, "/b"); got != "C" {
		t.Fatalf("txn /b = %q, want C", got)
	}
	if got := readString(t, txn, "/c"); got != "A" {
		t.Fatalf("txn /c = %q, want A", got)
	}
	mustNotExist(t, txn, "/tmp")

	// Base is still untouched.
	if got := readString(t, base, "/a"); got != "A" {
		t.Fatalf("base /a before commit = %q, want A", got)
	}
	mustNotExist(t, base, "/tmp")

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got := readString(t, base, "/a"); got != "B" {
		t.Fatalf("base /a after commit = %q, want B", got)
	}
	if got := readString(t, base, "/b"); got != "C" {
		t.Fatalf("base /b after commit = %q, want C", got)
	}
	if got := readString(t, base, "/c"); got != "A" {
		t.Fatalf("base /c after commit = %q, want A", got)
	}
	mustNotExist(t, base, "/tmp")
	if names := dirNames(t, base, "/"); !equalStrings(names, []string{"a", "b", "c"}) {
		t.Fatalf("base root listing after commit = %v, want [a b c]", names)
	}
}

// Committing (or rolling back) an already committed transaction must fail.
func TestTxnOverlayRepeatedCommit(t *testing.T) {
	base := setupTxnBase(t)
	txn := NewTxnOverlayFs(base)

	if err := WriteFile(txn, "/a", []byte("A2"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if err := txn.Commit(); !errors.Is(err, ErrTxnCommitted) {
		t.Fatalf("second commit err = %v, want ErrTxnCommitted", err)
	}
	if err := txn.Rollback(); !errors.Is(err, ErrTxnCommitted) {
		t.Fatalf("rollback after commit err = %v, want ErrTxnCommitted", err)
	}
	if err := WriteFile(txn, "/a", []byte("A3"), 0o644); !errors.Is(err, ErrTxnCommitted) {
		t.Fatalf("write after commit err = %v, want ErrTxnCommitted", err)
	}
	// The single commit was applied exactly once.
	if got := readString(t, base, "/a"); got != "A2" {
		t.Fatalf("base /a = %q, want A2", got)
	}
}

// Directory removal uses a whiteout marker: the base directory stays in
// place until commit, the transaction can recreate the directory, and the
// commit removes the old subtree wholesale.
func TestTxnOverlayDirWhiteout(t *testing.T) {
	base := NewMemMapFs()
	if err := base.MkdirAll("/dir", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"/dir/f1", "/dir/f2"} {
		if err := WriteFile(base, f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteFile(base, "/other", []byte("O"), 0o644); err != nil {
		t.Fatal(err)
	}

	txn := NewTxnOverlayFs(base)
	if err := txn.RemoveAll("/dir"); err != nil {
		t.Fatal(err)
	}

	// Transaction view: /dir is gone entirely.
	mustNotExist(t, txn, "/dir")
	mustNotExist(t, txn, "/dir/f1")
	if names := dirNames(t, txn, "/"); !equalStrings(names, []string{"other"}) {
		t.Fatalf("txn root listing = %v, want [other]", names)
	}

	// Base view: still there.
	mustExist(t, base, "/dir/f1")
	mustExist(t, base, "/dir/f2")

	// Recreate the directory inside the transaction with fresh content.
	if err := txn.Mkdir("/dir", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(txn, "/dir/new", []byte("N"), 0o644); err != nil {
		t.Fatal(err)
	}
	if names := dirNames(t, txn, "/dir"); !equalStrings(names, []string{"new"}) {
		t.Fatalf("txn /dir listing = %v, want [new]", names)
	}
	// The whiteout still hides the old base entries.
	mustNotExist(t, txn, "/dir/f1")

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Base view after commit: old subtree gone, new content in place.
	mustNotExist(t, base, "/dir/f1")
	mustNotExist(t, base, "/dir/f2")
	if got := readString(t, base, "/dir/new"); got != "N" {
		t.Fatalf("base /dir/new = %q, want N", got)
	}
	if names := dirNames(t, base, "/dir"); !equalStrings(names, []string{"new"}) {
		t.Fatalf("base /dir listing = %v, want [new]", names)
	}
	if got := readString(t, base, "/other"); got != "O" {
		t.Fatalf("base /other = %q, want O", got)
	}
}

// Reading a directory records its version: a concurrent base change to the
// directory's content is detected as a conflict.
func TestTxnOverlayDirReadConflict(t *testing.T) {
	base := setupTxnBase(t)
	txn := NewTxnOverlayFs(base)

	if names := dirNames(t, txn, "/"); !equalStrings(names, []string{"a", "b", "c"}) {
		t.Fatalf("txn root listing = %v, want [a b c]", names)
	}
	// Concurrent base change to the directory that was listed.
	if err := WriteFile(base, "/newfile", []byte("N"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); !errors.Is(err, ErrTxnConflict) {
		t.Fatalf("commit err = %v, want ErrTxnConflict", err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	// After the rollback the transaction observes the base as-is.
	if names := dirNames(t, txn, "/"); !equalStrings(names, []string{"a", "b", "c", "newfile"}) {
		t.Fatalf("txn root listing after rollback = %v", names)
	}
}

// A cross-directory rename stages parent directory metadata of both sides
// for conflict detection, and commits correctly.
func TestTxnOverlayCrossDirRename(t *testing.T) {
	base := setupTxnBase(t)
	if err := base.MkdirAll("/x", 0o755); err != nil {
		t.Fatal(err)
	}
	txn := NewTxnOverlayFs(base)

	if err := txn.Rename("/c", "/x/d"); err != nil {
		t.Fatal(err)
	}
	mustNotExist(t, txn, "/c")
	if got := readString(t, txn, "/x/d/f"); got != "F" {
		t.Fatalf("txn /x/d/f = %q, want F", got)
	}
	mustExist(t, base, "/c/f")
	mustNotExist(t, base, "/x/d")

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustNotExist(t, base, "/c")
	if got := readString(t, base, "/x/d/f"); got != "F" {
		t.Fatalf("base /x/d/f = %q, want F", got)
	}
}

// Metadata operations staged in the transaction are applied on commit.
func TestTxnOverlayMetadataOps(t *testing.T) {
	base := setupTxnBase(t)
	txn := NewTxnOverlayFs(base)

	if err := txn.Chmod("/a", 0o600); err != nil {
		t.Fatal(err)
	}
	fi, err := txn.Stat("/a")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("txn /a mode = %o, want 600", fi.Mode().Perm())
	}
	// Base unchanged before commit.
	bfi, err := base.Stat("/a")
	if err != nil {
		t.Fatal(err)
	}
	if bfi.Mode().Perm() != 0o644 {
		t.Fatalf("base /a mode before commit = %o, want 644", bfi.Mode().Perm())
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	bfi, err = base.Stat("/a")
	if err != nil {
		t.Fatal(err)
	}
	if bfi.Mode().Perm() != 0o600 {
		t.Fatalf("base /a mode after commit = %o, want 600", bfi.Mode().Perm())
	}
	if got := readString(t, base, "/a"); got != "A" {
		t.Fatalf("base /a content = %q, want A", got)
	}
}
