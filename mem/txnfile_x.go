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

package mem

// FullName returns the complete, rooted name of f (including its parent
// directories), as opposed to FileInfo.Name() which returns only the base
// name.
func FullName(f *FileData) string {
	return f.Name()
}

// DirFiles returns the direct children of a directory FileData. It returns
// nil for non-directory nodes.
func DirFiles(f *FileData) []*FileData {
	if f == nil || f.memDir == nil {
		return nil
	}
	return f.memDir.Files()
}

// ResetMemDir empties the directory listing of a directory FileData.
func ResetMemDir(f *FileData) {
	if f == nil {
		return
	}
	InitializeDir(f)
	f.memDir = &DirMap{}
	f.dir = true
}

// GetUID returns the owning uid recorded on f.
func GetUID(f *FileData) int {
	f.Lock()
	defer f.Unlock()
	return f.uid
}

// GetGID returns the owning gid recorded on f.
func GetGID(f *FileData) int {
	f.Lock()
	defer f.Unlock()
	return f.gid
}
