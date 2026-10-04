package afero

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// Metadata area inside the overlay layer that records the explicit
// deletion ("whiteout") mask of a CopyOnWriteFs. Everything below this
// reserved, hidden root belongs to the filesystem implementation: it is
// filtered from every view exposed to callers (Stat/Open/ReadDir/...) and
// must not be created, renamed or removed through the CopyOnWriteFs API.
var (
	cowMetaDir      = FilePathSeparator + ".__afero_cow__"
	cowWhiteoutFile = filepath.Join(cowMetaDir, "whiteouts")
)

// cowMetaVersion is the first line of the on-overlay mask storage.
const cowMetaVersion = "afero-copy-on-write-whiteouts v1"

// whiteoutStore keeps the set of normalized paths that have been explicitly
// deleted from the union file system. A deleted directory hides all of its
// base descendants; creating an entry at such a path (or below it) in the
// overlay locally restores only that entry.
//
// Storage format (one record per line, UTF-8 text, written into the overlay
// at cowWhiteoutFile):
//
//	# afero-copy-on-write-whiteouts v1
//	# one normalized, separator-cleaned absolute path per line
//	/d
//	/d/a
//
// Lines starting with '#' and blank lines are ignored when loading, which
// makes the format forward compatible with additional metadata.
type whiteoutStore struct {
	fs     Fs
	mu     sync.RWMutex
	paths  map[string]struct{}
	loaded bool
}

func newWhiteoutStore(layer Fs) *whiteoutStore {
	s := &whiteoutStore{fs: layer}
	s.load()
	return s
}

// norm normalizes a path the same way the layers do: filepath.Clean plus a
// leading separator for relative paths (see normalizePath in memmap.go).
func cowNorm(name string) string {
	if name == "" {
		return ""
	}
	name = filepath.Clean(name)
	switch name {
	case ".", "..":
		return FilePathSeparator
	}
	if !strings.HasPrefix(name, FilePathSeparator) {
		name = FilePathSeparator + name
	}
	return name
}

// samePath compares two normalized paths. Windows (and other case
// insensitive file systems) treat "D/A" and "d/a" as the same path, so the
// mask uses the same semantics the underlying file system uses.
func samePath(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

// isMeta reports whether name lives in the reserved metadata area.
func isMetaPath(name string) bool {
	name = cowNorm(name)
	if samePath(name, cowMetaDir) {
		return true
	}
	return isPathOrBelow(name, cowMetaDir)
}

// isPathOrBelow reports whether path equals or is a descendant of dir.
func isPathOrBelow(path, dir string) bool {
	if samePath(path, dir) {
		return true
	}
	prefix := dir
	if !strings.HasSuffix(prefix, FilePathSeparator) {
		prefix += FilePathSeparator
	}
	if runtime.GOOS == "windows" {
		return len(path) >= len(prefix) && strings.EqualFold(path[:len(prefix)], prefix)
	}
	return strings.HasPrefix(path, prefix)
}

func (s *whiteoutStore) load() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded {
		return
	}
	s.paths = make(map[string]struct{})
	s.loaded = true

	f, err := s.fs.Open(cowWhiteoutFile)
	if err != nil {
		// No mask recorded yet (or a layer that cannot be read at
		// construction time): start with an empty mask.
		return
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		s.paths[cowNorm(line)] = struct{}{}
	}
}

// snapshot returns the current mask as a sorted slice of normalized paths.
// Callers must not mutate the returned slice.
func (s *whiteoutStore) snapshotLocked() []string {
	out := make([]string, 0, len(s.paths))
	for p := range s.paths {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// persistLocked writes the whole mask back to the overlay metadata area.
// Callers hold s.mu.
func (s *whiteoutStore) persistLocked() error {
	var buf bytes.Buffer
	buf.WriteString("# ")
	buf.WriteString(cowMetaVersion)
	buf.WriteString("\n# one normalized absolute path per line\n")
	for _, p := range s.snapshotLocked() {
		buf.WriteString(p)
		buf.WriteByte('\n')
	}
	if err := s.fs.MkdirAll(cowMetaDir, 0o755); err != nil {
		return err
	}
	f, err := s.fs.OpenFile(cowWhiteoutFile,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(buf.Bytes())
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	return cerr
}

// add records path in the mask and persists the metadata. A failed persist
// rolls the in-memory mask back so a failed deletion is never observed as
// successful.
func (s *whiteoutStore) add(path string) error {
	path = cowNorm(path)
	if path == "" || path == FilePathSeparator || isMetaPath(path) {
		return syscall.EPERM
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.paths[path]; ok {
		return nil
	}
	s.paths[path] = struct{}{}
	if err := s.persistLocked(); err != nil {
		delete(s.paths, path)
		return err
	}
	return nil
}

// has reports whether the exact normalized path is masked.
func (s *whiteoutStore) has(path string) bool {
	path = cowNorm(path)
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.paths[path]
	if !ok && runtime.GOOS == "windows" {
		for p := range s.paths {
			if strings.EqualFold(p, path) {
				return true
			}
		}
	}
	return ok
}

// hidden reports whether a path that only exists in the base must be
// hidden: the path itself, or one of its ancestor directories, was deleted
// (and has not been overridden by an overlay entry, which callers check
// separately).
func (s *whiteoutStore) hidden(path string) bool {
	path = cowNorm(path)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for masked := range s.paths {
		if isPathOrBelow(path, masked) {
			return true
		}
	}
	return false
}

// hiddenChild is the per-directory-entry variant used while merging
// ReadDir results: dir is the directory being listed and name is the
// base-only child entry.
func (s *whiteoutStore) hiddenChild(dir, name string) bool {
	return s.hidden(filepath.Join(dir, name))
}

// maskedDirFile wraps a directory File of a single layer and filters its
// ReadDir/Readdirnames output so that whiteouted base descendants and the
// reserved metadata area are never listed. It makes filtering part of
// directory reads (not only of Open() on files).
type maskedDirFile struct {
	File
	u      *CopyOnWriteFs
	dir    string
	off    int
	loaded bool
	files  []os.FileInfo
}

func (f *maskedDirFile) visible(fi os.FileInfo) bool {
	name := fi.Name()
	if isMetaPath(filepath.Join(f.dir, name)) {
		return false
	}
	return !f.u.mask().hiddenChild(f.dir, name)
}

func (f *maskedDirFile) load() error {
	if f.loaded {
		return nil
	}
	f.loaded = true
	all, err := f.File.Readdir(-1)
	if err != nil {
		return err
	}
	for _, fi := range all {
		if f.visible(fi) {
			f.files = append(f.files, fi)
		}
	}
	return nil
}

// Readdir returns the filtered directory entries. At the end of the
// directory view, the error is io.EOF if c > 0.
func (f *maskedDirFile) Readdir(c int) ([]os.FileInfo, error) {
	if err := f.load(); err != nil {
		return nil, err
	}
	files := f.files[f.off:]
	if c <= 0 {
		return files, nil
	}
	if len(files) == 0 {
		return nil, io.EOF
	}
	if c > len(files) {
		c = len(files)
	}
	defer func() { f.off += c }()
	return files[:c], nil
}

func (f *maskedDirFile) Readdirnames(c int) ([]string, error) {
	rfi, err := f.Readdir(c)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(rfi))
	for _, fi := range rfi {
		names = append(names, fi.Name())
	}
	return names, nil
}
