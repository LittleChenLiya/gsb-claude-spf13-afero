// Command cowmask-demo demonstrates the CopyOnWriteFs deletion mask.
//
// It creates a base filesystem with d/a and d/b, deletes d, prints the
// persisted mask file, recreates d/a in the overlay, and prints the merged
// directory listing.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/afero"
)

func main() {
	base := afero.NewMemMapFs()
	layer := afero.NewMemMapFs()

	// Populate the read-only base.
	must(base.MkdirAll("d", 0o755))
	must(afero.WriteFile(base, "d/a", []byte("base-a"), 0o644))
	must(afero.WriteFile(base, "d/b", []byte("base-b"), 0o644))

	ufs := afero.NewCopyOnWriteFs(base, layer)

	fmt.Println("== Before deletion ==")
	printDir(ufs, "d")

	// Delete d; this records the mask in the overlay.
	must(ufs.RemoveAll("d"))

	fmt.Println("\n== After deleting d ==")
	fmt.Println("d/a exists:", exists(ufs, "d/a"))
	fmt.Println("d/b exists:", exists(ufs, "d/b"))

	// Show the persisted mask file.
	fmt.Println("\n== Mask file content (/.afero-cow-deletions) ==")
	data, err := afero.ReadFile(layer, "/.afero-cow-deletions")
	if err != nil {
		fmt.Println("read mask error:", err)
	} else {
		fmt.Print(string(data))
	}

	// Rebuild the filesystem to prove persistence.
	ufs2 := afero.NewCopyOnWriteFs(base, layer)
	fmt.Println("\n== After rebuild ==")
	fmt.Println("d/a exists:", exists(ufs2, "d/a"))
	fmt.Println("d/b exists:", exists(ufs2, "d/b"))

	// Recreate d/a in the overlay.
	must(ufs2.MkdirAll("d", 0o755))
	must(afero.WriteFile(ufs2, "d/a", []byte("overlay-a"), 0o644))

	fmt.Println("\n== After recreating d/a in overlay ==")
	printDir(ufs2, "d")
	fmt.Println("d/a exists:", exists(ufs2, "d/a"))
	fmt.Println("d/b exists:", exists(ufs2, "d/b"))
}

func printDir(fs afero.Fs, dir string) {
	entries, err := afero.ReadDir(fs, dir)
	if err != nil {
		fmt.Println("ReadDir error:", err)
		return
	}
	for _, e := range entries {
		fmt.Printf("  %s (dir=%v)\n", e.Name(), e.IsDir())
	}
}

func exists(fs afero.Fs, name string) bool {
	ok, _ := afero.Exists(fs, name)
	return ok
}

func must(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}
