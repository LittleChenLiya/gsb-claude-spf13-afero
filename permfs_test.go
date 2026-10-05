package afero

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func newTestPermFs(t *testing.T) (*PermFs, Fs) {
	t.Helper()
	mem := NewMemMapFs()
	if err := mem.MkdirAll("/ro", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := mem.MkdirAll("/rw", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(mem, "/ro/data.txt", []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(mem, "/rw/file.txt", []byte("rw-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	pfs := NewPermFs(mem,
		PermRule{Path: "/ro", Ops: PermRead},
		PermRule{Path: "/rw", Ops: PermRead | PermWrite | PermCreate | PermDelete},
	)
	return pfs, mem
}

func readString(t *testing.T, f File) string {
	t.Helper()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPermFsReadOnlyPath(t *testing.T) {
	pfs, mem := newTestPermFs(t)

	f, err := pfs.Open("/ro/data.txt")
	if err != nil {
		t.Fatalf("open read-only file: %v", err)
	}
	if got := readString(t, f); got != "hello" {
		t.Fatalf("read content = %q, want %q", got, "hello")
	}

	// Mutating calls on the handle follow the profile captured at Open.
	if _, err := f.Write([]byte("x")); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Write on read-only handle: got %v, want permission error", err)
	}
	if _, err := f.WriteString("x"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("WriteString on read-only handle: got %v, want permission error", err)
	}
	if _, err := f.WriteAt([]byte("x"), 0); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("WriteAt on read-only handle: got %v, want permission error", err)
	}
	if err := f.Truncate(0); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Truncate on read-only handle: got %v, want permission error", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// OpenFile with write flags is denied before touching the source.
	if _, err := pfs.OpenFile("/ro/data.txt", os.O_WRONLY, 0o644); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("OpenFile O_WRONLY on read-only path: got %v, want permission error", err)
	}
	if _, err := pfs.OpenFile("/ro/data.txt", os.O_RDWR, 0o644); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("OpenFile O_RDWR on read-only path: got %v, want permission error", err)
	}

	// The underlying MemMapFs content is unchanged.
	if got := readString(t, mustOpen(t, mem, "/ro/data.txt")); got != "hello" {
		t.Fatalf("source content = %q, want %q", got, "hello")
	}
}

func mustOpen(t *testing.T, fs Fs, name string) File {
	t.Helper()
	f, err := fs.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestPermFsRenameToUnauthorizedDir(t *testing.T) {
	pfs, mem := newTestPermFs(t)

	// Target directory has no PermCreate: denied, and no target file appears.
	if err := pfs.Rename("/rw/file.txt", "/ro/moved.txt"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("rename into read-only dir: got %v, want permission error", err)
	}
	if _, err := mem.Stat("/ro/moved.txt"); !os.IsNotExist(err) {
		t.Fatalf("denied rename created target: stat err = %v", err)
	}
	// Source is untouched.
	if got := readString(t, mustOpen(t, mem, "/rw/file.txt")); got != "rw-content" {
		t.Fatalf("source content = %q, want %q", got, "rw-content")
	}

	// Source directory has no PermDelete: denied as well.
	if err := pfs.Rename("/ro/data.txt", "/rw/copied.txt"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("rename out of read-only dir: got %v, want permission error", err)
	}
	if _, err := mem.Stat("/rw/copied.txt"); !os.IsNotExist(err) {
		t.Fatalf("denied rename created target: stat err = %v", err)
	}
	if got := readString(t, mustOpen(t, mem, "/ro/data.txt")); got != "hello" {
		t.Fatalf("source content = %q, want %q", got, "hello")
	}

	// A fully authorized rename works.
	if err := pfs.Rename("/rw/file.txt", "/rw/renamed.txt"); err != nil {
		t.Fatalf("authorized rename: %v", err)
	}
	if got := readString(t, mustOpen(t, mem, "/rw/renamed.txt")); got != "rw-content" {
		t.Fatalf("renamed content = %q, want %q", got, "rw-content")
	}
}

func TestPermFsDeniedOperationsCreateNothing(t *testing.T) {
	pfs, mem := newTestPermFs(t)

	if _, err := pfs.Create("/ro/new.txt"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Create on read-only path: got %v, want permission error", err)
	}
	if _, err := mem.Stat("/ro/new.txt"); !os.IsNotExist(err) {
		t.Fatalf("denied Create left a file behind: stat err = %v", err)
	}

	// O_CREATE on a missing file in a read-only dir must not create it.
	_, err := pfs.OpenFile("/ro/ghost.txt", os.O_WRONLY|os.O_CREATE, 0o644)
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("OpenFile O_CREATE on read-only path: got %v, want permission error", err)
	}
	if _, err := mem.Stat("/ro/ghost.txt"); !os.IsNotExist(err) {
		t.Fatalf("denied OpenFile created the target: stat err = %v", err)
	}

	// No rule at all: default deny, still nothing created.
	if _, err := pfs.Create("/elsewhere/x.txt"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Create without any rule: got %v, want permission error", err)
	}
	if _, err := mem.Stat("/elsewhere/x.txt"); !os.IsNotExist(err) {
		t.Fatalf("denied Create left a file behind: stat err = %v", err)
	}

	if err := pfs.Mkdir("/ro/sub", 0o755); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Mkdir on read-only path: got %v, want permission error", err)
	}
	if err := pfs.Remove("/ro/data.txt"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Remove on read-only path: got %v, want permission error", err)
	}
	if err := pfs.Chmod("/ro/data.txt", 0o600); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Chmod on read-only path: got %v, want permission error", err)
	}
	if got := readString(t, mustOpen(t, mem, "/ro/data.txt")); got != "hello" {
		t.Fatalf("source content = %q, want %q", got, "hello")
	}
}

func TestPermFsFourOperations(t *testing.T) {
	pfs, mem := newTestPermFs(t)

	// Create.
	f, err := pfs.Create("/rw/note.txt")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Write.
	if _, err := f.WriteString("note"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Truncate(4); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// Read.
	rf, err := pfs.Open("/rw/note.txt")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := readString(t, rf); got != "note" {
		t.Fatalf("read = %q, want %q", got, "note")
	}
	rf.Close()
	// Delete.
	if err := pfs.Remove("/rw/note.txt"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := mem.Stat("/rw/note.txt"); !os.IsNotExist(err) {
		t.Fatalf("file still present after Remove: %v", err)
	}
}

func TestPermFsHandleProfilePinnedAtAcquisition(t *testing.T) {
	pfs, mem := newTestPermFs(t)

	// Acquire a read-write handle on /rw, then move the file underneath it
	// into the read-only tree via the source Fs. The handle keeps the
	// profile captured at acquisition: writes still succeed.
	wf, err := pfs.OpenFile("/rw/file.txt", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := mem.Rename("/rw/file.txt", "/ro/moved.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := wf.WriteAt([]byte("RW"), 0); err != nil {
		t.Fatalf("write via pinned profile after rename: %v", err)
	}
	wf.Close()
	if got := readString(t, mustOpen(t, mem, "/ro/moved.txt")); got != "RW-content" {
		t.Fatalf("content after pinned write = %q, want %q", got, "RW-content")
	}

	// The other direction: a read-only handle stays read-only even if a
	// writable file is renamed over its path afterwards.
	rf, err := pfs.Open("/ro/data.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(mem, "/rw/evil.txt", []byte("evil"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := mem.Rename("/rw/evil.txt", "/ro/data.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := rf.Write([]byte("x")); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("write via read-only pinned handle: got %v, want permission error", err)
	}
	if err := rf.Truncate(0); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("truncate via read-only pinned handle: got %v, want permission error", err)
	}
	rf.Close()
	if got := readString(t, mustOpen(t, mem, "/ro/data.txt")); got != "evil" {
		t.Fatalf("content = %q, want %q (untouched by denied writes)", got, "evil")
	}
}

func TestPermFsHandleOrigin(t *testing.T) {
	pfs, mem := newTestPermFs(t)

	f, err := pfs.Open("/ro/data.txt")
	if err != nil {
		t.Fatal(err)
	}
	pf, ok := f.(*PermFile)
	if !ok {
		t.Fatalf("expected *PermFile, got %T", f)
	}
	want := filepath.Clean("/ro/data.txt")
	if pf.Origin() != want {
		t.Fatalf("Origin() = %q, want %q", pf.Origin(), want)
	}
	if pf.Profile() != PermRead {
		t.Fatalf("Profile() = %v, want PermRead", pf.Profile())
	}
	// Renaming the file does not rewrite the handle provenance.
	if err := mem.Rename("/ro/data.txt", "/ro/other.txt"); err != nil {
		t.Fatal(err)
	}
	if pf.Origin() != want {
		t.Fatalf("Origin() after rename = %q, want %q", pf.Origin(), want)
	}
	f.Close()
}

func TestPermFsBorrowedHandleClosePolicy(t *testing.T) {
	mem := NewMemMapFs()
	if err := WriteFile(mem, "/b.txt", []byte("borrowed"), 0o644); err != nil {
		t.Fatal(err)
	}

	raw, err := mem.Open("/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	// Borrowed handle, ownClose=false: Close is a no-op, the lender keeps
	// ownership and the underlying file stays usable.
	borrowed := WrapPermFile(raw, "/b.txt", PermRead, false)
	if err := borrowed.Close(); err != nil {
		t.Fatalf("borrowed Close: %v", err)
	}
	if got := readString(t, raw); got != "borrowed" {
		t.Fatalf("underlying file after borrowed Close = %q, want %q", got, "borrowed")
	}
	raw.Close()

	// ownClose=true: Close propagates to the underlying file.
	raw2, err := mem.Open("/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	owned := WrapPermFile(raw2, "/b.txt", PermRead, true)
	if err := owned.Close(); err != nil {
		t.Fatalf("owned Close: %v", err)
	}
	if _, err := raw2.Read(make([]byte, 1)); err == nil {
		t.Fatal("underlying file still readable after owned Close")
	}
}

func TestPermFsLongestRuleWins(t *testing.T) {
	mem := NewMemMapFs()
	if err := mem.MkdirAll("/a/b", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(mem, "/a/b/c.txt", []byte("c"), 0o644); err != nil {
		t.Fatal(err)
	}
	pfs := NewPermFs(mem,
		PermRule{Path: "/a", Ops: PermRead | PermWrite | PermCreate | PermDelete},
		PermRule{Path: "/a/b", Ops: PermRead},
	)
	if _, err := pfs.OpenFile("/a/b/c.txt", os.O_WRONLY, 0o644); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("inner read-only rule should win: got %v", err)
	}
	if _, err := pfs.Open("/a/b/c.txt"); err != nil {
		t.Fatalf("read allowed by inner rule: %v", err)
	}
	if _, err := pfs.Create("/a/d.txt"); err != nil {
		t.Fatalf("outer rule still applies elsewhere: %v", err)
	}
}
