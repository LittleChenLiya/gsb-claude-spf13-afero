package afero

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
)

func writeBaseFile(t *testing.T, fs Fs, name, content string) {
	t.Helper()
	if err := fs.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(fs, name, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func listNames(t *testing.T, fs Fs, dir string) []string {
	t.Helper()
	f, err := fs.Open(dir)
	if err != nil {
		t.Fatalf("open %q: %v", dir, err)
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		t.Fatalf("readdir %q: %v", dir, err)
	}
	sort.Strings(names)
	return names
}

func containsName(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// TestCopyOnWriteWhiteoutAcceptance is the required acceptance scenario:
// base holds d/a and d/b, deleting d hides both, recreating d/a in the
// overlay restores only a, and the mask survives rebuilding the file system.
func TestCopyOnWriteWhiteoutAcceptance(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	writeBaseFile(t, base, "d/a", "A")
	writeBaseFile(t, base, "d/b", "B")

	fs1 := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)

	if err := fs1.RemoveAll("d"); err != nil {
		t.Fatalf("RemoveAll(d): %v", err)
	}

	for _, p := range []string{"d", "d/a", "d/b"} {
		if _, err := fs1.Stat(p); !os.IsNotExist(err) {
			t.Errorf("Stat(%q) after delete: got %v, want not-exist", p, err)
		}
		if f, err := fs1.Open(p); err == nil {
			f.Close()
			t.Errorf("Open(%q) after delete succeeded, want not-exist", p)
		}
	}
	if names := listNames(t, fs1, "."); containsName(names, "d") {
		t.Errorf("root listing still shows deleted d: %v", names)
	}
	var walked []string
	_ = Walk(fs1, "/", func(path string, info os.FileInfo, err error) error {
		if err == nil {
			walked = append(walked, path)
		}
		return nil
	})
	for _, p := range walked {
		if strings.HasPrefix(p, "/d") {
			t.Errorf("Walk visited deleted path %q in %v", p, walked)
		}
	}

	if err := fs1.MkdirAll("d", 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := fs1.Create("d/a")
	if err != nil {
		t.Fatalf("Create(d/a): %v", err)
	}
	if _, err := f.Write([]byte("newA")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if names := listNames(t, fs1, "d"); len(names) != 1 || names[0] != "a" {
		t.Fatalf("d listing after recreating a: %v, want [a]", names)
	}
	if _, err := fs1.Stat("d/b"); !os.IsNotExist(err) {
		t.Errorf("Stat(d/b): got %v, want not-exist", err)
	}
	if got, err := ReadFile(fs1, "d/a"); err != nil || string(got) != "newA" {
		t.Errorf("ReadFile(d/a) = %q, %v", got, err)
	}

	// Close and rebuild the file system on top of the same overlay layer.
	fs2 := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)
	if names := listNames(t, fs2, "d"); len(names) != 1 || names[0] != "a" {
		t.Fatalf("after rebuild d listing: %v, want [a]", names)
	}
	if _, err := fs2.Stat("d/b"); !os.IsNotExist(err) {
		t.Errorf("after rebuild Stat(d/b): got %v, want not-exist", err)
	}
	if _, err := fs2.Stat("d/a"); err != nil {
		t.Errorf("after rebuild Stat(d/a): %v", err)
	}
}

func TestCopyOnWriteWhiteoutRemoveFile(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	writeBaseFile(t, base, "keep", "k")
	writeBaseFile(t, base, "gone", "g")

	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)
	if err := fs.Remove("gone"); err != nil {
		t.Fatalf("Remove(gone): %v", err)
	}
	if _, err := fs.Stat("gone"); !os.IsNotExist(err) {
		t.Errorf("Stat(gone): %v", err)
	}
	// Deleting again stays successful (the entry stays hidden).
	if err := fs.Remove("gone"); err != nil {
		t.Errorf("second Remove(gone): %v", err)
	}
	// Removing a name that exists nowhere fails and records nothing.
	if err := fs.Remove("nope"); !os.IsNotExist(err) {
		t.Errorf("Remove(nope) = %v, want ENOENT", err)
	}
	if fs.(*CopyOnWriteFs).mask().has("/nope") {
		t.Error("failed removal recorded a whiteout")
	}
	if names := listNames(t, fs, "."); containsName(names, "gone") || !containsName(names, "keep") {
		t.Errorf("root listing = %v, want keep without gone", names)
	}
}

// failingMaskFs wraps a layer whose metadata write fails, proving that a
// failed deletion never records a successful mask.
type failingMaskFs struct {
	Fs
}

func (f failingMaskFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if strings.Contains(name, cowWhiteoutFile) && flag&os.O_WRONLY != 0 {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrPermission}
	}
	return f.Fs.OpenFile(name, flag, perm)
}

func TestCopyOnWriteWhiteoutFailedDelete(t *testing.T) {
	base := &MemMapFs{}
	layer := failingMaskFs{Fs: &MemMapFs{}}
	writeBaseFile(t, base, "x", "X")

	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)
	if err := fs.Remove("x"); err == nil {
		t.Fatal("Remove(x) with failing mask store succeeded")
	}
	if _, err := fs.Stat("x"); err != nil {
		t.Errorf("base entry hidden despite failed deletion: %v", err)
	}
	if fs.(*CopyOnWriteFs).mask().has("/x") {
		t.Error("failed deletion recorded a whiteout in memory")
	}
	if f, err := layer.Open(cowWhiteoutFile); err == nil {
		data, _ := io.ReadAll(f)
		f.Close()
		if strings.Contains(string(data), "/x") {
			t.Errorf("failed deletion persisted mask: %q", data)
		}
	}
}

func TestCopyOnWriteWhiteoutRename(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	writeBaseFile(t, base, "a", "A")
	writeBaseFile(t, base, "b", "B")
	writeBaseFile(t, base, "dir/f", "F")

	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)

	// Renaming a base-only file copies it up and masks the old name.
	if err := fs.Rename("a", "c"); err != nil {
		t.Fatalf("Rename(a,c): %v", err)
	}
	if _, err := fs.Stat("a"); !os.IsNotExist(err) {
		t.Errorf("Stat(a) after rename: %v", err)
	}
	if got, err := ReadFile(fs, "c"); err != nil || string(got) != "A" {
		t.Errorf("ReadFile(c) = %q, %v", got, err)
	}

	// Renaming over another base file masks the destination base entry.
	if err := fs.Rename("c", "b"); err != nil {
		t.Fatalf("Rename(c,b): %v", err)
	}
	if got, err := ReadFile(fs, "b"); err != nil || string(got) != "A" {
		t.Errorf("ReadFile(b) = %q, %v", got, err)
	}
	if names := listNames(t, fs, "."); containsName(names, "a") || containsName(names, "c") {
		t.Errorf("listing after renames = %v", names)
	}

	// Renaming a base-only directory stays rejected and masks nothing.
	if err := fs.Rename("dir", "dir2"); err != syscall.EPERM {
		t.Errorf("Rename(dir,dir2) = %v, want EPERM", err)
	}
	if _, err := fs.Stat("dir/f"); err != nil {
		t.Errorf("base dir hidden after rejected rename: %v", err)
	}

	// Renaming a missing name fails without writing a mask.
	if err := fs.Rename("missing", "x"); !os.IsNotExist(err) {
		t.Errorf("Rename(missing,x) = %v, want ENOENT", err)
	}
}

func TestCopyOnWriteWhiteoutMetaInvisible(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	writeBaseFile(t, base, "d/a", "A")
	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)
	if err := fs.RemoveAll("d"); err != nil {
		t.Fatal(err)
	}

	for _, p := range []string{cowMetaDir, cowWhiteoutFile} {
		if _, err := fs.Stat(p); !os.IsNotExist(err) {
			t.Errorf("Stat(%s) = %v, want not-exist", p, err)
		}
		if f, err := fs.Open(p); err == nil {
			f.Close()
			t.Errorf("Open(%s) succeeded", p)
		}
		if err := fs.Remove(p); err != syscall.EPERM {
			t.Errorf("Remove(%s) = %v, want EPERM", p, err)
		}
		if _, err := fs.Create(p); err != syscall.EPERM {
			t.Errorf("Create(%s) = %v, want EPERM", p, err)
		}
		if err := fs.Mkdir(p, 0o755); err != syscall.EPERM {
			t.Errorf("Mkdir(%s) = %v, want EPERM", p, err)
		}
	}
	if names := listNames(t, fs, "."); containsName(names, strings.TrimPrefix(cowMetaDir, "/")) {
		t.Errorf("metadata dir listed: %v", names)
	}

	// The storage physically exists in the overlay in the documented format.
	f, err := layer.Open(cowWhiteoutFile)
	if err != nil {
		t.Fatalf("mask not persisted in overlay: %v", err)
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	text := string(data)
	if !strings.HasPrefix(text, "# "+cowMetaVersion) {
		t.Errorf("mask file missing version header: %q", text)
	}
	if !strings.Contains(text, "\n"+cowNorm("/d")+"\n") {
		t.Errorf("mask file missing d record: %q", text)
	}
}

func TestCopyOnWriteWhiteoutNormalization(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	writeBaseFile(t, base, "d/a", "A")
	writeBaseFile(t, base, "d/b", "B")
	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)

	// A messy relative path cleans to the same normalized d.
	if err := fs.RemoveAll(filepath.Join("d", "x", "..")); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat("d/a"); !os.IsNotExist(err) {
		t.Errorf("normalized delete did not mask d/a: %v", err)
	}
	store := fs.(*CopyOnWriteFs).whiteouts
	if !store.has("/d") {
		t.Error("mask not stored under normalized path")
	}
}

func TestCopyOnWriteWhiteoutCaseSemantics(t *testing.T) {
	store := newWhiteoutStore(&MemMapFs{})
	if err := store.add("/Case/File"); err != nil {
		t.Fatal(err)
	}

	if runtime.GOOS == "windows" {
		if !store.hidden("/case/file") || !store.hidden("/CASE/FILE") {
			t.Error("mask should be case insensitive on Windows")
		}
	} else {
		if store.hidden("/case/file") {
			t.Error("mask should be case sensitive off Windows")
		}
		if !store.hidden("/Case/File") {
			t.Error("exact-case mask should always match")
		}
	}
	// Descendant hiding works regardless of casing.
	if !store.hidden("/Case/File/sub") {
		t.Error("descendant of masked dir not hidden")
	}
}

func TestCopyOnWriteWhiteoutResurrectFile(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	writeBaseFile(t, base, "f", "base")
	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)

	if err := fs.Remove("f"); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(fs, "f", []byte("overlay"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, _ := ReadFile(fs, "f"); string(got) != "overlay" {
		t.Errorf("resurrected content = %q", got)
	}
	if err := fs.Remove("f"); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat("f"); !os.IsNotExist(err) {
		t.Errorf("base file reappeared after deleting overlay copy: %v", err)
	}
}

// TestCopyOnWriteWhiteoutMergedReaddir lists a directory that exists in
// both layers after masking some base entries: overlay entries must win,
// masked base entries must vanish and pagination must stay consistent.
func TestCopyOnWriteWhiteoutMergedReaddir(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	writeBaseFile(t, base, "m/base1", "1")
	writeBaseFile(t, base, "m/base2", "2")
	writeBaseFile(t, base, "m/gone", "g")
	// Overlay also has the directory plus its own file.
	writeBaseFile(t, layer, "m/layer1", "l")
	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)

	if err := fs.Remove("m/gone"); err != nil {
		t.Fatal(err)
	}
	names := listNames(t, fs, "m")
	sort.Strings(names)
	want := []string{"base1", "base2", "layer1"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("merged listing = %v, want %v", names, want)
	}

	// Paginated Readdir must return the same filtered set without EOF
	// leaking the hidden entry or duplicating anything.
	f, err := fs.Open("m")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var paged []string
	for {
		batch, err := f.Readdirnames(2)
		paged = append(paged, batch...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(paged)
	if strings.Join(paged, ",") != strings.Join(want, ",") {
		t.Errorf("paged listing = %v, want %v", paged, want)
	}
}

// TestCopyOnWriteWhiteoutNestedDelete masks a nested directory while the
// parent stays visible, and restores one grandchild locally.
func TestCopyOnWriteWhiteoutNestedDelete(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	writeBaseFile(t, base, "p/del/a", "a")
	writeBaseFile(t, base, "p/del/b", "b")
	writeBaseFile(t, base, "p/keep", "k")
	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)

	if err := fs.RemoveAll("p/del"); err != nil {
		t.Fatal(err)
	}
	if names := listNames(t, fs, "p"); len(names) != 1 || names[0] != "keep" {
		t.Errorf("p listing = %v, want [keep]", names)
	}
	if _, err := fs.Stat("p/del/a"); !os.IsNotExist(err) {
		t.Errorf("masked grandchild visible: %v", err)
	}

	// Locally recreate p/del/a via Create (parent auto-created in overlay).
	f, err := fs.Create("p/del/a")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if names := listNames(t, fs, "p/del"); len(names) != 1 || names[0] != "a" {
		t.Errorf("p/del listing = %v, want [a]", names)
	}
	if names := listNames(t, fs, "p"); len(names) != 2 {
		t.Errorf("p listing after restore = %v, want del and keep", names)
	}
}
