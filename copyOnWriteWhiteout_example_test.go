package afero

import (
	"fmt"
	"os"
	"sort"
)

// ExampleCopyOnWriteFs_deletionMask is a small demonstration of the explicit
// deletion ("whiteout") mask of CopyOnWriteFs:
//
//   - deleting a directory hides its read-only base descendants;
//   - recreating a single entry in the overlay restores only that entry;
//   - the mask lives in a hidden metadata area of the overlay and therefore
//     stays in effect after closing and rebuilding the file system.
func ExampleCopyOnWriteFs_deletionMask() {
	base := &MemMapFs{}
	layer := &MemMapFs{}
	for _, p := range []string{"d/a", "d/b"} {
		_ = base.MkdirAll("d", 0o755)
		_ = WriteFile(base, p, []byte(p), 0o644)
	}

	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)

	// Delete a directory that only exists in the read-only base.
	if err := fs.RemoveAll("d"); err != nil {
		fmt.Println("remove error:", err)
	}
	_, err := fs.Stat("d/a")
	fmt.Println("d/a visible after delete:", err == nil)

	// Recreate just d/a locally in the overlay.
	_ = fs.MkdirAll("d", 0o755)
	_ = WriteFile(fs, "d/a", []byte("new"), 0o644)

	f, _ := fs.Open("d")
	names, _ := f.Readdirnames(-1)
	f.Close()
	sort.Strings(names)
	fmt.Println("d contents after recreate:", names)

	// Close and rebuild on the same overlay: the mask is reloaded.
	fs2 := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)
	_, err = fs2.Stat("d/b")
	fmt.Println("d/b still hidden after rebuild:", os.IsNotExist(err))
	// Output:
	// d/a visible after delete: false
	// d contents after recreate: [a]
	// d/b still hidden after rebuild: true
}
