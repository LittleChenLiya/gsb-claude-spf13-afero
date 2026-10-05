package afero

import (
	"fmt"
	"os"
	"testing"
)

// newPermTestBase builds a MemMapFs with a read-only directory, a writable
// directory and an existing file containing "original".
func newPermTestBase(t *testing.T) *MemMapFs {
	t.Helper()
	base := NewMemMapFs().(*MemMapFs)
	for _, d := range []string{"/ro", "/rw", "/del"} {
		if err := base.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", d, err)
		}
	}
	f, err := base.Create("/ro/file.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.Write([]byte("original")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f.Close()
	df, err := base.Create("/del/file.txt")
	if err != nil {
		t.Fatalf("Create /del/file.txt: %v", err)
	}
	df.Close()
	return base
}

func readAllContent(t *testing.T, fs Fs, name string) string {
	t.Helper()
	f, err := fs.Open(name)
	if err != nil {
		t.Fatalf("Open %s: %v", name, err)
	}
	defer f.Close()
	buf := make([]byte, 64)
	n, _ := f.Read(buf)
	return string(buf[:n])
}

// TestPermFsReadOnlyPath is the acceptance scenario: a read-only path can
// be opened for reading, but Write and Truncate on the handle are rejected;
// renaming to an unauthorized directory does not create a file; and the
// MemMapFs original content stays unchanged.
func TestPermFsReadOnlyPath(t *testing.T) {
	base := newPermTestBase(t)
	fs := NewPermFs(base).
		Allow("/ro", PermRead).
		Allow("/rw", PermWrite|PermCreate)

	rf, err := fs.Open("/ro/file.txt")
	if err != nil {
		t.Fatalf("Open on read-only path failed: %v", err)
	}
	if got := readAllContent(t, fs, "/ro/file.txt"); got != "original" {
		t.Fatalf("Read content = %q, want %q", got, "original")
	}

	if _, err := rf.Write([]byte("hack")); err == nil {
		t.Error("Write on read-only handle should be denied, got nil")
	} else if !os.IsPermission(err) {
		t.Errorf("Write error = %v, want permission error", err)
	}

	if err := rf.Truncate(0); err == nil {
		t.Error("Truncate on read-only handle should be denied, got nil")
	} else if !os.IsPermission(err) {
		t.Errorf("Truncate error = %v, want permission error", err)
	}
	rf.Close()

	if err := fs.Rename("/ro/file.txt", "/rw/file.txt"); err == nil {
		t.Error("Rename to unauthorized directory should be denied, got nil")
	}
	if _, err := base.Stat("/rw/file.txt"); !os.IsNotExist(err) {
		t.Errorf("Target file /rw/file.txt was created by denied rename, stat err = %v", err)
	}

	if got := readAllContent(t, base, "/ro/file.txt"); got != "original" {
		t.Errorf("MemMapFs content changed to %q, want %q", got, "original")
	}
}

// TestPermFsCreateDeniedDoesNotCreate verifies that a denied OpenFile with
// O_CREATE does not create the target file in the source filesystem.
func TestPermFsCreateDeniedDoesNotCreate(t *testing.T) {
	base := newPermTestBase(t)
	fs := NewPermFs(base).Allow("/ro", PermRead|PermWrite)

	if _, err := fs.OpenFile("/ro/new.txt", os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
		t.Error("OpenFile with O_CREATE on path without create permission should fail")
	}
	if _, err := base.Stat("/ro/new.txt"); !os.IsNotExist(err) {
		t.Errorf("Denied OpenFile created /ro/new.txt, stat err = %v", err)
	}
}

// TestPermFsOpenTruncateDeniedDoesNotTruncate verifies that a denied
// O_TRUNC open leaves the existing file content untouched.
func TestPermFsOpenTruncateDeniedDoesNotTruncate(t *testing.T) {
	base := newPermTestBase(t)
	fs := NewPermFs(base).Allow("/ro", PermRead)

	if _, err := fs.OpenFile("/ro/file.txt", os.O_TRUNC|os.O_WRONLY, 0o644); err == nil {
		t.Error("OpenFile with O_TRUNC on read-only path should fail")
	}
	if got := readAllContent(t, base, "/ro/file.txt"); got != "original" {
		t.Errorf("Content after denied O_TRUNC = %q, want %q", got, "original")
	}
}

// TestPermFsHandleSourcePreserved verifies that the wrapped handle keeps
// its borrowed source handle and the name captured at issuance, so a later
// rename in the source filesystem cannot change the handle origin.
func TestPermFsHandleSourcePreserved(t *testing.T) {
	base := newPermTestBase(t)
	fs := NewPermFs(base).Allow("/ro", PermRead)

	rf, err := fs.Open("/ro/file.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	pf, ok := rf.(*PermFile)
	if !ok {
		t.Fatalf("Returned handle is %T, want *PermFile", rf)
	}
	if pf.Source() == nil {
		t.Error("Source() returned nil; borrowed handle origin is not preserved")
	}
	if pf.Name() != "/ro/file.txt" {
		t.Errorf("Name() = %q, want %q", pf.Name(), "/ro/file.txt")
	}

	if err := base.Rename("/ro/file.txt", "/ro/moved.txt"); err != nil {
		t.Fatalf("Source rename: %v", err)
	}
	if pf.Name() != "/ro/file.txt" {
		t.Errorf("Name() after source rename = %q, want captured %q", pf.Name(), "/ro/file.txt")
	}
	if got := readAllContent(t, base, "/ro/moved.txt"); got != "original" {
		t.Errorf("Moved file content = %q, want %q", got, "original")
	}
	rf.Close()
}

// TestPermFsBorrowedHandleClose verifies the close policy: the borrowed
// source handle is closed exactly once, closing is idempotent and later
// operations report ErrFileClosed.
func TestPermFsBorrowedHandleClose(t *testing.T) {
	base := newPermTestBase(t)
	fs := NewPermFs(base).Allow("/ro", PermRead)

	rf, err := fs.Open("/ro/file.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := rf.Close(); err != nil {
		t.Errorf("First Close() = %v, want nil", err)
	}
	if err := rf.Close(); err != ErrFileClosed {
		t.Errorf("Second Close() = %v, want ErrFileClosed", err)
	}
	if _, err := rf.Read(make([]byte, 4)); err != ErrFileClosed {
		t.Errorf("Read after Close() = %v, want ErrFileClosed", err)
	}
}

// TestPermFsWriteAllowed verifies that write and truncate succeed on a
// handle issued for a writable path.
func TestPermFsWriteAllowed(t *testing.T) {
	base := newPermTestBase(t)
	wf0, err := base.Create("/rw/file.txt")
	if err != nil {
		t.Fatalf("base.Create: %v", err)
	}
	wf0.Close()
	fs := NewPermFs(base).Allow("/rw", PermRead|PermWrite)

	wf, err := fs.OpenFile("/rw/file.txt", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := wf.Write([]byte("hello")); err != nil {
		t.Errorf("Write on writable handle: %v", err)
	}
	if err := wf.Truncate(2); err != nil {
		t.Errorf("Truncate on writable handle: %v", err)
	}
	wf.Close()
	if got := readAllContent(t, base, "/rw/file.txt"); got != "he" {
		t.Errorf("Content = %q, want %q", got, "he")
	}
}

// TestPermFsDeleteAndRename verifies the delete category and that rename
// requires delete on the source plus create on the destination.
func TestPermFsDeleteAndRename(t *testing.T) {
	base := newPermTestBase(t)
	fs := NewPermFs(base).Allow("/del", PermRead|PermWrite|PermCreate|PermDelete)

	if err := fs.Remove("/del/file.txt"); err != nil {
		t.Errorf("Remove with delete permission: %v", err)
	}
	if _, err := base.Stat("/del/file.txt"); !os.IsNotExist(err) {
		t.Errorf("Removed file still exists, stat err = %v", err)
	}

	if err := fs.Rename("/ro/file.txt", "/del/renamed.txt"); err == nil {
		t.Error("Rename out of /ro (no delete) should be denied")
	}
	if _, err := base.Stat("/del/renamed.txt"); !os.IsNotExist(err) {
		t.Errorf("Denied rename created /del/renamed.txt, stat err = %v", err)
	}

	if err := fs.Rename("/del", "/del/archive"); err != nil {
		t.Errorf("Rename with delete+create: %v", err)
	}
	if _, err := base.Stat("/del/archive"); err != nil {
		t.Errorf("Rename target missing: %v", err)
	}
}

// TestPermFsMkdir verifies the create category for directories.
func TestPermFsMkdir(t *testing.T) {
	base := NewMemMapFs()
	fs := NewPermFs(base).Allow("/d", PermCreate)

	if err := fs.Mkdir("/d/sub", 0o755); err != nil {
		t.Errorf("Mkdir with create permission: %v", err)
	}
	if err := fs.Mkdir("/other", 0o755); err == nil {
		t.Error("Mkdir without create permission should be denied")
	}
	if _, err := base.Stat("/other"); !os.IsNotExist(err) {
		t.Errorf("Denied Mkdir created /other, stat err = %v", err)
	}
}

// TestPermFsDefaultDeny verifies that a wrapper with no rules denies
// everything.
func TestPermFsDefaultDeny(t *testing.T) {
	base := newPermTestBase(t)
	fs := NewPermFs(base)

	if _, err := fs.Open("/ro/file.txt"); !os.IsPermission(err) {
		t.Errorf("Open with no rules = %v, want permission error", err)
	}
	if _, err := fs.Create("/ro/x.txt"); !os.IsPermission(err) {
		t.Errorf("Create with no rules = %v, want permission error", err)
	}
	if err := fs.Remove("/ro/file.txt"); !os.IsPermission(err) {
		t.Errorf("Remove with no rules = %v, want permission error", err)
	}
	if err := fs.Rename("/ro/file.txt", "/rw/x.txt"); !os.IsPermission(err) {
		t.Errorf("Rename with no rules = %v, want permission error", err)
	}
}

// TestPermFsCanonicalPath verifies that path cleaning cannot bypass the
// rules: "/ro/../rw" is authorized as /rw, not as /ro.
func TestPermFsCanonicalPath(t *testing.T) {
	base := newPermTestBase(t)
	wf0, err := base.Create("/rw/file.txt")
	if err != nil {
		t.Fatalf("base.Create: %v", err)
	}
	wf0.Close()
	fs := NewPermFs(base).Allow("/ro", PermRead).Allow("/rw", PermRead|PermWrite)

	wf, err := fs.OpenFile("/ro/../rw/file.txt", os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile through canonical path: %v", err)
	}
	if _, err := wf.Write([]byte("xy")); err != nil {
		t.Errorf("Write through canonical /rw path: %v", err)
	}
	wf.Close()

	rf, err := fs.Open("/ro/../ro/file.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := rf.Write([]byte("z")); !os.IsPermission(err) {
		t.Errorf("Write through canonical /ro path = %v, want permission error", err)
	}
	rf.Close()
	if got := readAllContent(t, base, "/ro/file.txt"); got != "original" {
		t.Errorf("Content after traversal write attempt = %q, want %q", got, "original")
	}
}

// TestPermFsDenyRule verifies that explicit Deny rules take precedence.
func TestPermFsDenyRule(t *testing.T) {
	base := newPermTestBase(t)
	fs := NewPermFs(base).
		Allow("/", PermAll).
		Deny("/ro", PermWrite|PermDelete)

	if _, err := fs.Open("/ro/file.txt"); err != nil {
		t.Errorf("Open under broad allow: %v", err)
	}
	if _, err := fs.OpenFile("/ro/file.txt", os.O_WRONLY, 0o644); !os.IsPermission(err) {
		t.Errorf("Write under deny rule = %v, want permission error", err)
	}
	if err := fs.Remove("/ro/file.txt"); !os.IsPermission(err) {
		t.Errorf("Remove under deny rule = %v, want permission error", err)
	}
}

// ExamplePermFs demonstrates the four operation categories: read, write,
// create and delete, plus an out-of-policy write being rejected.
func ExamplePermFs() {
	base := NewMemMapFs()
	_ = base.MkdirAll("/srv/read", 0o755)
	_ = base.MkdirAll("/srv/write", 0o755)
	_ = base.MkdirAll("/srv/delete", 0o755)
	rf, _ := base.Create("/srv/read/hello.txt")
	_, _ = rf.Write([]byte("hello"))
	rf.Close()
	_, _ = base.Create("/srv/write/notes.txt")
	_, _ = base.Create("/srv/delete/gone.txt")

	fs := NewPermFs(base).
		Allow("/srv/read", PermRead).
		Allow("/srv/write", PermWrite).
		Allow("/srv/create", PermCreate).
		Allow("/srv/delete", PermDelete)

	// Read: a file under the read grant can be opened and read.
	f, err := fs.Open("/srv/read/hello.txt")
	fmt.Println("read open ok:", err == nil)
	buf := make([]byte, 5)
	n, _ := f.Read(buf)
	fmt.Println("read bytes:", n)
	f.Close()

	// Write: a handle issued for a writable path accepts writes.
	wf, err := fs.OpenFile("/srv/write/notes.txt", os.O_WRONLY, 0o644)
	fmt.Println("write open ok:", err == nil)
	n, _ = wf.Write([]byte("ok"))
	fmt.Println("write bytes:", n)
	wf.Close()

	// Create: the create grant allows a new file to be made.
	cf, err := fs.Create("/srv/create/new.txt")
	fmt.Println("create ok:", err == nil)
	cf.Close()

	// Delete: the delete grant allows removing a file.
	err = fs.Remove("/srv/delete/gone.txt")
	fmt.Println("delete ok:", err == nil)

	// Out-of-policy: a read-only handle rejects writes without touching
	// the underlying file.
	rf2, _ := fs.Open("/srv/read/hello.txt")
	_, err = rf2.Write([]byte("x"))
	fmt.Println("write on read-only denied:", err != nil)
	rf2.Close()

	// Output:
	// read open ok: true
	// read bytes: 5
	// write open ok: true
	// write bytes: 2
	// create ok: true
	// delete ok: true
	// write on read-only denied: true
}
