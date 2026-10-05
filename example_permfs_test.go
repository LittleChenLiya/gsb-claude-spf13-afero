package afero_test

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/afero"
)

// ExamplePermFs demonstrates the four operation categories (create, write,
// read, delete) on an explicitly authorized directory, plus a denied access
// outside the allow-list.
func ExamplePermFs() {
	fs := afero.NewPermFs(afero.NewMemMapFs(),
		afero.PermRule{
			Path: "/data",
			Ops:  afero.PermRead | afero.PermWrite | afero.PermCreate | afero.PermDelete,
		},
	)

	// Create + Write.
	f, err := fs.Create("/data/note.txt")
	if err != nil {
		fmt.Println("create:", err)
		return
	}
	if _, err := f.WriteString("hello"); err != nil {
		fmt.Println("write:", err)
		return
	}
	f.Close()

	// Read.
	rf, err := fs.Open("/data/note.txt")
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	content, _ := io.ReadAll(rf)
	rf.Close()
	fmt.Println("read:", string(content))

	// Delete.
	if err := fs.Remove("/data/note.txt"); err != nil {
		fmt.Println("remove:", err)
		return
	}
	fmt.Println("delete: ok")

	// Anything outside the allow-list is denied, and nothing is created.
	_, err = fs.Create("/secret.txt")
	fmt.Println("denied:", errors.Is(err, os.ErrPermission))

	// Output:
	// read: hello
	// delete: ok
	// denied: true
}
