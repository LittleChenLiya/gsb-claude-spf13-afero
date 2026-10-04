package afero

import (
	"errors"
	"strings"
	"testing"
)

// TestCopyOnWriteFs_DeleteMask is the acceptance test.
func TestCopyOnWriteFs_DeleteMask(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)
	WriteFile(base, "d/b", []byte("b"), 0o644)

	ufs := NewCopyOnWriteFs(base, layer)

	for _, name := range []string{"d/a", "d/b"} {
		if ok, _ := Exists(ufs, name); !ok {
			t.Fatalf("expected %s to exist before deletion", name)
		}
	}

	if err := ufs.RemoveAll("d"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	for _, name := range []string{"d/a", "d/b"} {
		if ok, _ := Exists(ufs, name); ok {
			t.Errorf("expected %s to be hidden after deletion", name)
		}
	}

	if _, err := ReadDir(ufs, "d"); err == nil {
		t.Errorf("expected ReadDir on masked directory to fail")
	}

	ufs.MkdirAll("d", 0o755)
	WriteFile(ufs, "d/a", []byte("a"), 0o644)

	entries, err := ReadDir(ufs, "d")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a" {
		t.Errorf("expected only a to appear, got %v", entries)
	}

	if ok, _ := Exists(ufs, "d/a"); !ok {
		t.Errorf("expected d/a to exist after recreation")
	}
	if ok, _ := Exists(ufs, "d/b"); ok {
		t.Errorf("expected d/b to remain hidden")
	}
}

// TestCopyOnWriteFs_DeleteMask_Persistence verifies mask survives rebuild.
func TestCopyOnWriteFs_DeleteMask_Persistence(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)
	WriteFile(base, "d/b", []byte("b"), 0o644)

	ufs := NewCopyOnWriteFs(base, layer)
	if err := ufs.RemoveAll("d"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	ufs2 := NewCopyOnWriteFs(base, layer)

	for _, name := range []string{"d/a", "d/b"} {
		if ok, _ := Exists(ufs2, name); ok {
			t.Errorf("expected %s to be hidden after rebuild", name)
		}
	}
}

// TestCopyOnWriteFs_DeleteMask_SingleFile verifies deleting one base file.
func TestCopyOnWriteFs_DeleteMask_SingleFile(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)
	WriteFile(base, "d/b", []byte("b"), 0o644)

	ufs := NewCopyOnWriteFs(base, layer)

	if err := ufs.Remove("d/a"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if ok, _ := Exists(ufs, "d/a"); ok {
		t.Errorf("expected d/a to be hidden")
	}
	if ok, _ := Exists(ufs, "d/b"); !ok {
		t.Errorf("expected d/b to remain visible")
	}

	entries, err := ReadDir(ufs, "d")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "b" {
		t.Errorf("expected only b to appear, got %v", entries)
	}
}

// TestCopyOnWriteFs_DeleteMask_RecreateAfterDelete verifies recreate then delete.
func TestCopyOnWriteFs_DeleteMask_RecreateAfterDelete(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)

	ufs := NewCopyOnWriteFs(base, layer)

	if err := ufs.Remove("d/a"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if ok, _ := Exists(ufs, "d/a"); ok {
		t.Errorf("expected d/a to be hidden after deletion")
	}

	ufs.MkdirAll("d", 0o755)
	WriteFile(ufs, "d/a", []byte("new-a"), 0o644)
	if ok, _ := Exists(ufs, "d/a"); !ok {
		t.Errorf("expected d/a to exist after recreation")
	}

	if err := ufs.Remove("d/a"); err != nil {
		t.Fatalf("Remove overlay file failed: %v", err)
	}
	if ok, _ := Exists(ufs, "d/a"); ok {
		t.Errorf("expected d/a to be hidden after second deletion")
	}
}

// TestCopyOnWriteFs_DeleteMask_Rename verifies Rename semantics with mask.
func TestCopyOnWriteFs_DeleteMask_Rename(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)

	ufs := NewCopyOnWriteFs(base, layer)

	if err := ufs.RemoveAll("d"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	ufs.MkdirAll("d", 0o755)
	WriteFile(ufs, "d/a", []byte("a"), 0o644)

	if err := ufs.Rename("d/a", "d/c"); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if ok, _ := Exists(ufs, "d/c"); !ok {
		t.Errorf("expected d/c to be visible after rename")
	}
	if ok, _ := Exists(ufs, "d/a"); ok {
		t.Errorf("expected d/a to be hidden after rename")
	}
}

// TestCopyOnWriteFs_DeleteMask_CaseSensitivity verifies case path semantics.
func TestCopyOnWriteFs_DeleteMask_CaseSensitivity(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)
	base.MkdirAll("D", 0o755)
	WriteFile(base, "D/b", []byte("b"), 0o644)

	ufs := NewCopyOnWriteFs(base, layer)

	if err := ufs.RemoveAll("d"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	if ok, _ := Exists(ufs, "d/a"); ok {
		t.Errorf("expected d/a to be hidden")
	}
	if ok, _ := Exists(ufs, "D/b"); !ok {
		t.Errorf("expected D/b to remain visible (case-sensitive)")
	}
}

// TestCopyOnWriteFs_DeleteMask_FailedDeletion verifies failed delete no mask.
func TestCopyOnWriteFs_DeleteMask_FailedDeletion(t *testing.T) {
	base := NewMemMapFs()
	layer := &failRemoveFs{MemMapFs: NewMemMapFs().(*MemMapFs)}

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)

	layer.MkdirAll("d", 0o755)
	WriteFile(layer, "d/a", []byte("overlay-a"), 0o644)

	ufs := NewCopyOnWriteFs(base, layer)

	if err := ufs.Remove("d/a"); err == nil {
		t.Fatalf("expected Remove to fail")
	}

	maskData, err := ReadFile(layer, cowMaskPath)
	if err == nil {
		for _, line := range strings.Split(string(maskData), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if normalizeCowPath(line) == normalizeCowPath("d/a") {
				t.Errorf("mask should not contain d/a after failed deletion")
			}
		}
	}
}

// TestCopyOnWriteFs_DeleteMask_StorageFormat verifies mask storage format.
func TestCopyOnWriteFs_DeleteMask_StorageFormat(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)
	WriteFile(base, "d/b", []byte("b"), 0o644)

	ufs := NewCopyOnWriteFs(base, layer)

	if err := ufs.RemoveAll("d"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	maskData, err := ReadFile(layer, cowMaskPath)
	if err != nil {
		t.Fatalf("ReadFile mask failed: %v", err)
	}

	expected := map[string]bool{
		normalizeCowPath("d"):   true,
		normalizeCowPath("d/a"): true,
		normalizeCowPath("d/b"): true,
	}
	for _, line := range strings.Split(string(maskData), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := normalizeCowPath(line)
		if !expected[p] {
			t.Errorf("unexpected path in mask: %q", p)
		}
		delete(expected, p)
	}
	if len(expected) > 0 {
		t.Errorf("missing paths in mask: %v", expected)
	}

	entries, err := ReadDir(ufs, "/")
	if err != nil {
		t.Fatalf("ReadDir union failed: %v", err)
	}
	for _, e := range entries {
		if e.Name() == ".afero-cow-deletions" {
			t.Errorf("mask file should be invisible to traversal")
		}
	}
}

// TestCopyOnWriteFs_DeleteMask_ReadOnlyBase verifies mask with read-only base.
func TestCopyOnWriteFs_DeleteMask_ReadOnlyBase(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	base.MkdirAll("d", 0o755)
	WriteFile(base, "d/a", []byte("a"), 0o644)
	WriteFile(base, "d/b", []byte("b"), 0o644)

	roBase := NewReadOnlyFs(base)
	ufs := NewCopyOnWriteFs(roBase, layer)

	if err := ufs.RemoveAll("d"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	for _, name := range []string{"d/a", "d/b"} {
		if ok, _ := Exists(ufs, name); ok {
			t.Errorf("expected %s to be hidden", name)
		}
	}

	ufs.MkdirAll("d", 0o755)
	WriteFile(ufs, "d/a", []byte("a"), 0o644)

	entries, err := ReadDir(ufs, "d")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a" {
		t.Errorf("expected only a, got %v", entries)
	}
}

// failRemoveFs wraps a MemMapFs and fails all Remove calls.
type failRemoveFs struct {
	*MemMapFs
}

func (f *failRemoveFs) Remove(name string) error {
	return errors.New("remove failed")
}
