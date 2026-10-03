// Copyright © 2026 Steve Francia < spf@spf13.com>.
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

package afero_test

import (
	"fmt"
	"os"

	"github.com/spf13/afero"
)

// ExampleSnapshot_Diff demonstrates capturing two read-only snapshots and
// printing the differences between them.
func ExampleSnapshot_Diff() {
	oldFs := afero.NewMemMapFs()
	newFs := afero.NewMemMapFs()

	// Populate the old tree.
	mustWrite(oldFs, "/tree/removed.txt", []byte("gone"))
	mustWrite(oldFs, "/tree/same.txt", []byte("same"))
	mustWrite(oldFs, "/tree/changed.txt", []byte("old"))
	mustMkdir(newFs, "/tree")
	mustMkdir(oldFs, "/tree")

	// Populate the new tree.
	mustWrite(newFs, "/tree/added.txt", []byte("new"))
	mustWrite(newFs, "/tree/same.txt", []byte("same"))
	mustWrite(newFs, "/tree/changed.txt", []byte("new"))

	oldSnap, err := afero.NewSnapshot(oldFs, "/tree")
	if err != nil {
		fmt.Println("snapshot error:", err)
		return
	}
	newSnap, err := afero.NewSnapshot(newFs, "/tree")
	if err != nil {
		fmt.Println("snapshot error:", err)
		return
	}

	diffs := oldSnap.Diff(newSnap)
	afero.PrintDiffs(os.Stdout, diffs)

	// Output:
	// + added           added.txt
	// ~ content_changed changed.txt
	// - removed         removed.txt
}

func mustWrite(fs afero.Fs, path string, data []byte) {
	if err := afero.WriteFile(fs, path, data, 0o644); err != nil {
		panic(err)
	}
}

func mustMkdir(fs afero.Fs, path string) {
	if err := fs.MkdirAll(path, 0o755); err != nil {
		panic(err)
	}
}
