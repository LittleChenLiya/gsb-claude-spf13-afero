package afero

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

var _ Lstater = (*CopyOnWriteFs)(nil)

// cowMaskPath is the path in the overlay where the deletion mask is stored.
// It lives in the overlay's independent metadata area and is filtered out of
// all directory listings so it remains invisible to external traversal.
const cowMaskPath = "/.afero-cow-deletions"

// The CopyOnWriteFs is a union filesystem: a read only base file system with
// a possibly writeable layer on top. Changes to the file system will only
// be made in the overlay: Changing an existing file in the base layer which
// is not present in the overlay will copy the file to the overlay ("changing"
// includes also calls to e.g. Chtimes(), Chmod() and Chown()).
//
// Deleting a file that exists only in the base layer records an explicit
// deletion mask in the overlay's metadata area; the file will not reappear
// on subsequent reads. Deleting a directory masks the directory and all of
// its base descendants. Recreating a file in the overlay partially restores
// it (the overlay version wins), while other old descendants remain masked.
// The mask is persisted in the overlay so it survives closing and rebuilding
// the filesystem.
//
// Reading directories is currently only supported via Open(), not OpenFile().
type CopyOnWriteFs struct {
	base       Fs
	layer      Fs
	mu         sync.Mutex
	mask       map[string]bool
	maskLoaded bool
}

func NewCopyOnWriteFs(base Fs, layer Fs) Fs {
	return &CopyOnWriteFs{base: base, layer: layer}
}

// normalizeCowPath normalizes a path for use in the deletion mask. Paths are
// cleaned, rooted, and expressed with forward slashes. On case-insensitive
// platforms (Windows) the path is lowercased so that "D/A" and "d/a" refer
// to the same masked entry.
func normalizeCowPath(path string) string {
	if path == "" {
		return ""
	}
	path = filepath.Clean(path)
	path = filepath.ToSlash(path)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if runtime.GOOS == "windows" {
		path = strings.ToLower(path)
	}
	return path
}

// loadMask loads the deletion mask from the overlay's metadata file. If the
// file does not exist the mask is empty. The result is cached; call
// reloadMask to force a re-read.
func (u *CopyOnWriteFs) loadMask() error {
	if u.maskLoaded {
		return nil
	}
	u.mask = make(map[string]bool)
	f, err := u.layer.Open(cowMaskPath)
	if err != nil {
		if os.IsNotExist(err) {
			u.maskLoaded = true
			return nil
		}
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		u.mask[normalizeCowPath(line)] = true
	}
	u.maskLoaded = true
	return scanner.Err()
}

// reloadMask forces the next isMasked/addMask call to re-read the mask
// from the overlay.
func (u *CopyOnWriteFs) reloadMask() {
	u.mu.Lock()
	u.maskLoaded = false
	u.mask = nil
	u.mu.Unlock()
}

// saveMask persists the deletion mask to the overlay's metadata file.
func (u *CopyOnWriteFs) saveMask() error {
	f, err := u.layer.OpenFile(cowMaskPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "# afero CopyOnWriteFs deletion mask\n# one normalized path per line\n"); err != nil {
		return err
	}
	for path := range u.mask {
		if _, err := fmt.Fprintf(f, "%s\n", path); err != nil {
			return err
		}
	}
	return nil
}

// isMasked reports whether the normalized path is recorded in the deletion
// mask. The mask is loaded lazily from the overlay on first access.
func (u *CopyOnWriteFs) isMasked(name string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.loadMask(); err != nil {
		return false
	}
	return u.mask[normalizeCowPath(name)]
}

// addMask records a single path in the deletion mask and persists it.
func (u *CopyOnWriteFs) addMask(name string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.loadMask(); err != nil {
		return err
	}
	u.mask[normalizeCowPath(name)] = true
	return u.saveMask()
}

// addMaskRecursive records a path and all of its base descendants in the
// deletion mask. This is used when deleting a directory so that the entire
// base subtree is hidden; recreating individual files in the overlay still
// partially restores them because the overlay takes precedence.
func (u *CopyOnWriteFs) addMaskRecursive(name string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.loadMask(); err != nil {
		return err
	}
	name = normalizeCowPath(name)
	u.mask[name] = true
	descendants, err := u.enumerateBaseDescendants(name)
	if err == nil {
		for _, desc := range descendants {
			if desc != cowMaskPath {
				u.mask[desc] = true
			}
		}
	}
	return u.saveMask()
}

// enumerateBaseDescendants returns all descendants of name present in the
// base layer. It walks the base directory tree; if the base cannot be
// walked (e.g. it does not exist) the returned slice is empty and the error
// is returned.
func (u *CopyOnWriteFs) enumerateBaseDescendants(name string) ([]string, error) {
	var descendants []string
	err := Walk(u.base, name, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path != name {
			descendants = append(descendants, normalizeCowPath(path))
		}
		return nil
	})
	return descendants, err
}

// newMergeFn returns a DirsMerger that filters out base entries recorded in
// the deletion mask and hides the mask metadata file from overlay listings.
func (u *CopyOnWriteFs) newMergeFn(dir string) DirsMerger {
	return func(lofi, bofi []os.FileInfo) ([]os.FileInfo, error) {
		filteredLofi := make([]os.FileInfo, 0, len(lofi))
		for _, fi := range lofi {
			if normalizeCowPath(filepath.Join(dir, fi.Name())) != cowMaskPath {
				filteredLofi = append(filteredLofi, fi)
			}
		}
		filteredBofi := make([]os.FileInfo, 0, len(bofi))
		for _, fi := range bofi {
			name := normalizeCowPath(filepath.Join(dir, fi.Name()))
			if !u.isMasked(name) {
				filteredBofi = append(filteredBofi, fi)
			}
		}
		return defaultUnionMergeDirsFn(filteredLofi, filteredBofi)
	}
}

// Returns true if the file is not in the overlay. A masked path behaves as
// if it were not present in the overlay so that write operations recreate
// it in the overlay (partial restore) instead of copying from the base.
func (u *CopyOnWriteFs) isBaseFile(name string) (bool, error) {
	if u.isMasked(name) {
		return false, nil
	}
	if _, err := u.layer.Stat(name); err == nil {
		return false, nil
	}
	_, err := u.base.Stat(name)
	if err != nil {
		if oerr, ok := err.(*os.PathError); ok {
			if oerr.Err == os.ErrNotExist || oerr.Err == syscall.ENOENT ||
				oerr.Err == syscall.ENOTDIR {
				return false, nil
			}
		}
		if err == syscall.ENOENT {
			return false, nil
		}
	}
	return true, err
}

func (u *CopyOnWriteFs) copyToLayer(name string) error {
	return copyToLayer(u.base, u.layer, name)
}

func (u *CopyOnWriteFs) Chtimes(name string, atime, mtime time.Time) error {
	b, err := u.isBaseFile(name)
	if err != nil {
		return err
	}
	if b {
		if err := u.copyToLayer(name); err != nil {
			return err
		}
	}
	return u.layer.Chtimes(name, atime, mtime)
}

func (u *CopyOnWriteFs) Chmod(name string, mode os.FileMode) error {
	b, err := u.isBaseFile(name)
	if err != nil {
		return err
	}
	if b {
		if err := u.copyToLayer(name); err != nil {
			return err
		}
	}
	return u.layer.Chmod(name, mode)
}

func (u *CopyOnWriteFs) Chown(name string, uid, gid int) error {
	b, err := u.isBaseFile(name)
	if err != nil {
		return err
	}
	if b {
		if err := u.copyToLayer(name); err != nil {
			return err
		}
	}
	return u.layer.Chown(name, uid, gid)
}

func (u *CopyOnWriteFs) Stat(name string) (os.FileInfo, error) {
	fi, err := u.layer.Stat(name)
	if err != nil {
		isNotExist := u.isNotExist(err)
		if isNotExist {
			if u.isMasked(name) {
				return nil, os.ErrNotExist
			}
			return u.base.Stat(name)
		}
		return nil, err
	}
	return fi, nil
}

func (u *CopyOnWriteFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	llayer, ok1 := u.layer.(Lstater)
	lbase, ok2 := u.base.(Lstater)

	if ok1 {
		fi, b, err := llayer.LstatIfPossible(name)
		if err == nil {
			return fi, b, nil
		}

		if !u.isNotExist(err) {
			return nil, b, err
		}
	}

	if ok2 {
		fi, b, err := lbase.LstatIfPossible(name)
		if err == nil {
			if u.isMasked(name) {
				return nil, b, os.ErrNotExist
			}
			return fi, b, nil
		}
		if !u.isNotExist(err) {
			return nil, b, err
		}
	}

	fi, err := u.Stat(name)

	return fi, false, err
}

func (u *CopyOnWriteFs) SymlinkIfPossible(oldname, newname string) error {
	if slayer, ok := u.layer.(Linker); ok {
		return slayer.SymlinkIfPossible(oldname, newname)
	}

	return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: ErrNoSymlink}
}

func (u *CopyOnWriteFs) ReadlinkIfPossible(name string) (string, error) {
	if rlayer, ok := u.layer.(LinkReader); ok {
		return rlayer.ReadlinkIfPossible(name)
	}

	if rbase, ok := u.base.(LinkReader); ok {
		if u.isMasked(name) {
			return "", os.ErrNotExist
		}
		return rbase.ReadlinkIfPossible(name)
	}

	return "", &os.PathError{Op: "readlink", Path: name, Err: ErrNoReadlink}
}

func (u *CopyOnWriteFs) isNotExist(err error) bool {
	if e, ok := err.(*os.PathError); ok {
		err = e.Err
	}
	if err == os.ErrNotExist || err == syscall.ENOENT || err == syscall.ENOTDIR {
		return true
	}
	return false
}

// Rename renames a file in the overlay. Renaming a file present only in the
// base layer is not permitted. A masked path behaves as if it did not exist.
func (u *CopyOnWriteFs) Rename(oldname, newname string) error {
	b, err := u.isBaseFile(oldname)
	if err != nil {
		return err
	}
	if b {
		if u.isMasked(oldname) {
			return os.ErrNotExist
		}
		return syscall.EPERM
	}
	return u.layer.Rename(oldname, newname)
}

// Remove removes a file. If the file exists in the overlay it is removed
// from the overlay; if it also exists in the base it is recorded in the
// deletion mask so it does not reappear. If the file exists only in the
// base it is recorded in the deletion mask. A failed overlay removal does
// not record a mask.
func (u *CopyOnWriteFs) Remove(name string) error {
	overlayExists, err := Exists(u.layer, name)
	if err != nil {
		return err
	}
	if overlayExists {
		if err := u.layer.Remove(name); err != nil {
			return err
		}
		baseExists, err := Exists(u.base, name)
		if err != nil {
			return err
		}
		if baseExists {
			if err := u.addMaskRecursive(name); err != nil {
				return err
			}
		}
		return nil
	}
	baseExists, err := Exists(u.base, name)
	if err != nil {
		return err
	}
	if baseExists {
		return u.addMaskRecursive(name)
	}
	return syscall.ENOENT
}

// RemoveAll removes a directory path and any children it contains. If the
// path exists in the overlay it is removed from the overlay; if it also
// exists in the base it is recorded (along with all base descendants) in
// the deletion mask. If the path exists only in the base it is recorded in
// the deletion mask. A failed overlay removal does not record a mask.
func (u *CopyOnWriteFs) RemoveAll(name string) error {
	overlayExists, err := Exists(u.layer, name)
	if err != nil {
		return err
	}
	if overlayExists {
		if err := u.layer.RemoveAll(name); err != nil {
			return err
		}
		baseExists, err := Exists(u.base, name)
		if err != nil {
			return err
		}
		if baseExists {
			if err := u.addMaskRecursive(name); err != nil {
				return err
			}
		}
		return nil
	}
	baseExists, err := Exists(u.base, name)
	if err != nil {
		return err
	}
	if baseExists {
		return u.addMaskRecursive(name)
	}
	return nil
}

func (u *CopyOnWriteFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	b, err := u.isBaseFile(name)
	if err != nil {
		return nil, err
	}

	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		if b {
			if err = u.copyToLayer(name); err != nil {
				return nil, err
			}
			return u.layer.OpenFile(name, flag, perm)
		}

		dir := filepath.Dir(name)
		isaDir, err := IsDir(u.base, dir)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if isaDir {
			if err = u.layer.MkdirAll(dir, 0o777); err != nil {
				return nil, err
			}
			return u.layer.OpenFile(name, flag, perm)
		}

		isaDir, err = IsDir(u.layer, dir)
		if err != nil {
			return nil, err
		}
		if isaDir {
			return u.layer.OpenFile(name, flag, perm)
		}

		return nil, &os.PathError{
			Op:   "open",
			Path: name,
			Err:  syscall.ENOTDIR,
		}
	}
	if b {
		if u.isMasked(name) {
			return nil, os.ErrNotExist
		}
		return u.base.OpenFile(name, flag, perm)
	}
	return u.layer.OpenFile(name, flag, perm)
}

// This function handles the 9 different possibilities caused
// by the union which are the intersection of the following...
//
//	layer: doesn't exist, exists as a file, and exists as a directory
//	base:  doesn't exist, exists as a file, and exists as a directory
func (u *CopyOnWriteFs) Open(name string) (File, error) {
	// Since the overlay overrides the base we check that first
	b, err := u.isBaseFile(name)
	if err != nil {
		return nil, err
	}

	// If overlay doesn't exist, check the base (base state irrelevant)
	if b {
		if u.isMasked(name) {
			return nil, os.ErrNotExist
		}
		// If the base entry is a directory, return a UnionFile with an
		// empty layer so the merger filters out masked base entries.
		if dir, derr := IsDir(u.base, name); derr == nil && dir {
			bfile, bErr := u.base.Open(name)
			if bErr != nil {
				return nil, bErr
			}
			return &UnionFile{Base: bfile, Layer: nil, Merger: u.newMergeFn(name)}, nil
		}
		return u.base.Open(name)
	}

	// If overlay is a file, return it (base state irrelevant)
	dir, err := IsDir(u.layer, name)
	if err != nil {
		if u.isMasked(name) {
			return nil, os.ErrNotExist
		}
		return u.layer.Open(name)
	}
	if !dir {
		return u.layer.Open(name)
	}

	// Overlay is a directory, base state now matters.
	// Base state has 3 states to check but 2 outcomes:
	// A. It's a file or non-readable in the base (return just the overlay)
	// B. It's an accessible directory in the base (return a UnionFile)

	// If base is file or nonreadable, return overlay
	dir, err = IsDir(u.base, name)
	if !dir || err != nil {
		return u.layer.Open(name)
	}

	// Both base & layer are directories
	// Return union file (if opens are without error)
	bfile, bErr := u.base.Open(name)
	lfile, lErr := u.layer.Open(name)

	// If either have errors at this point something is very wrong. Return nil and the errors
	if bErr != nil || lErr != nil {
		return nil, fmt.Errorf("BaseErr: %v\nOverlayErr: %v", bErr, lErr)
	}

	return &UnionFile{Base: bfile, Layer: lfile, Merger: u.newMergeFn(name)}, nil
}

func (u *CopyOnWriteFs) Mkdir(name string, perm os.FileMode) error {
	dir, err := IsDir(u.base, name)
	if err != nil {
		return u.layer.MkdirAll(name, perm)
	}
	if dir {
		return ErrFileExists
	}
	return u.layer.MkdirAll(name, perm)
}

func (u *CopyOnWriteFs) Name() string {
	return "CopyOnWriteFs"
}

func (u *CopyOnWriteFs) MkdirAll(name string, perm os.FileMode) error {
	dir, err := IsDir(u.base, name)
	if err != nil {
		return u.layer.MkdirAll(name, perm)
	}
	if dir {
		// This is in line with how os.MkdirAll behaves.
		return nil
	}
	return u.layer.MkdirAll(name, perm)
}

func (u *CopyOnWriteFs) Create(name string) (File, error) {
	return u.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o666)
}
