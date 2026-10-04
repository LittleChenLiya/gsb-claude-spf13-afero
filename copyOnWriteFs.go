package afero

import (
	"os"
	"path/filepath"
	"syscall"
	"time"
)

var _ Lstater = (*CopyOnWriteFs)(nil)

// The CopyOnWriteFs is a union filesystem: a read only base file system with
// a possibly writeable layer on top. Changes to the file system will only
// be made in the overlay: Changing an existing file in the base layer which
// is not present in the overlay will copy the file to the overlay ("changing"
// includes also calls to e.g. Chtimes(), Chmod() and Chown()).
//
// Reading directories is currently only supported via Open(), not OpenFile().
//
// Files and directories deleted from a read-only base are recorded in an
// explicit "whiteout" mask that lives in a hidden metadata area of the
// overlay layer (see copyOnWriteWhiteout.go). The mask survives closing and
// rebuilding the file system, hides all base descendants of a deleted
// directory and is merged/filtered into ReadDir results, not only Open().
type CopyOnWriteFs struct {
	base      Fs
	layer     Fs
	whiteouts *whiteoutStore
}

func NewCopyOnWriteFs(base Fs, layer Fs) Fs {
	return &CopyOnWriteFs{
		base:      base,
		layer:     layer,
		whiteouts: newWhiteoutStore(layer),
	}
}

// mask returns the whiteout store, creating it lazily for CopyOnWriteFs
// values built via struct literals (e.g. in tests).
func (u *CopyOnWriteFs) mask() *whiteoutStore {
	if u.whiteouts == nil {
		u.whiteouts = newWhiteoutStore(u.layer)
	}
	return u.whiteouts
}

// inLayer reports whether name exists in the overlay. The reserved metadata
// area is never considered part of the visible overlay.
func (u *CopyOnWriteFs) inLayer(name string) (bool, error) {
	if isMetaPath(name) {
		return false, nil
	}
	if _, err := u.layer.Stat(name); err == nil {
		return true, nil
	} else if !u.isNotExist(err) {
		return false, err
	}
	return false, nil
}

// notExistErr builds the canonical "no such file or directory" error for name.
func notExistErr(op, name string) error {
	return &os.PathError{Op: op, Path: name, Err: syscall.ENOENT}
}

// Returns true if the file is not in the overlay
func (u *CopyOnWriteFs) isBaseFile(name string) (bool, error) {
	if present, err := u.inLayer(name); err != nil {
		return false, err
	} else if present {
		return false, nil
	}
	if u.mask().hidden(name) {
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

// mutateBase checks whether a metadata changing operation is allowed on
// name: the reserved metadata area is never mutable and paths masked by a
// deletion behave as if they did not exist.
func (u *CopyOnWriteFs) mutateBase(op, name string) error {
	if isMetaPath(name) {
		return syscall.EPERM
	}
	present, err := u.inLayer(name)
	if err != nil {
		return err
	}
	if !present && u.mask().hidden(name) {
		return notExistErr(op, name)
	}
	return nil
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
	if err := u.mutateBase("chtimes", name); err != nil {
		return err
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
	if err := u.mutateBase("chmod", name); err != nil {
		return err
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
	if err := u.mutateBase("chown", name); err != nil {
		return err
	}
	return u.layer.Chown(name, uid, gid)
}

func (u *CopyOnWriteFs) Stat(name string) (os.FileInfo, error) {
	if isMetaPath(name) {
		// Metadata is private: never surface it from the overlay, but a
		// same-named base entry stays reachable.
		if fi, err := u.base.Stat(name); err == nil {
			return fi, nil
		}
		return nil, notExistErr("stat", name)
	}
	fi, err := u.layer.Stat(name)
	if err != nil {
		isNotExist := u.isNotExist(err)
		if isNotExist {
			if u.mask().hidden(name) {
				return nil, notExistErr("stat", name)
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

	if ok1 && !isMetaPath(name) {
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
			if !isMetaPath(name) && u.mask().hidden(name) {
				return nil, b, notExistErr("lstat", name)
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

// Rename handles the whiteout mask in addition to the layers:
//   - renaming an entry that exists in the overlay whiteouts the source name
//     (if it also existed in the base) and whiteouts the destination name
//     (if it shadows a base entry), then performs the overlay rename;
//   - renaming a file present only in the base copies it to the overlay
//     under the new name and records a whiteout for the old name;
//   - renaming a directory present only in the base is not permitted;
//   - the reserved metadata area cannot be renamed.
func (u *CopyOnWriteFs) Rename(oldname, newname string) error {
	if isMetaPath(oldname) || isMetaPath(newname) {
		return syscall.EPERM
	}

	inLayer, err := u.inLayer(oldname)
	if err != nil {
		return err
	}

	if inLayer {
		baseOld := existsIn(u.base, oldname)
		baseNew := existsIn(u.base, newname)
		if err := u.layer.Rename(oldname, newname); err != nil {
			return err
		}
		// Only record successful renames in the mask; if persistence fails
		// undo the overlay rename so the view matches the stored mask.
		if baseOld {
			if err := u.mask().add(oldname); err != nil {
				_ = u.layer.Rename(newname, oldname)
				return err
			}
		}
		if baseNew {
			if err := u.mask().add(newname); err != nil {
				_ = u.layer.Rename(newname, oldname)
				return err
			}
		}
		return nil
	}

	// Source is only in the base (or masked / missing).
	bfi, err := u.base.Stat(oldname)
	if err != nil {
		if u.isNotExist(err) {
			return notExistErr("rename", oldname)
		}
		return err
	}
	if bfi.IsDir() {
		// Moving a base-only directory would require copying and
		// whiteouting every descendant, which Rename never did.
		return syscall.EPERM
	}

	// Copy the base file to the overlay under the new name, then whiteout
	// the old name. Any failure before the mask is written leaves no mask.
	if err := copyToLayer(u.base, u.layer, oldname); err != nil {
		return err
	}
	if err := u.layer.Rename(oldname, newname); err != nil {
		return err
	}
	if err := u.mask().add(oldname); err != nil {
		return err
	}
	if existsIn(u.base, newname) {
		if err := u.mask().add(newname); err != nil {
			return err
		}
	}
	return nil
}

// existsIn reports whether name is visible in fs (ignoring not-exist errors).
func existsIn(fs Fs, name string) bool {
	_, err := fs.Stat(name)
	return err == nil
}

// Remove removes the overlay entry (if any) and, when the name also exists in
// the read-only base, records a whiteout mask so the base entry stays hidden.
// Removing a non-existent name fails with ENOENT and records nothing; a
// failure never records a successful mask.
func (u *CopyOnWriteFs) Remove(name string) error {
	if isMetaPath(name) {
		return syscall.EPERM
	}
	layerErr := u.layer.Remove(name)
	inLayer := layerErr == nil
	if !inLayer && !u.isNotExist(layerErr) {
		return layerErr
	}

	baseExists := existsIn(u.base, name)
	masked := u.mask().has(name)

	if !inLayer && !baseExists && !masked {
		return notExistErr("remove", name)
	}

	if baseExists || masked {
		if err := u.mask().add(name); err != nil {
			// Roll back: nothing was changed in the base, and the overlay
			// removal already happened; surface the metadata failure rather
			// than pretending the deletion succeeded.
			return err
		}
	}
	return nil
}

// RemoveAll behaves like Remove for the whole subtree: overlay entries are
// deleted and base-only names are hidden through the whiteout mask. It is
// idempotent and removes nothing from the read-only base.
func (u *CopyOnWriteFs) RemoveAll(name string) error {
	if isMetaPath(name) {
		return syscall.EPERM
	}
	if cowNorm(name) == FilePathSeparator {
		return syscall.EPERM
	}

	inLayer, err := u.inLayer(name)
	if err != nil {
		return err
	}
	baseExists := existsIn(u.base, name)
	masked := u.mask().has(name)

	if !inLayer && !baseExists && !masked {
		return notExistErr("remove", name)
	}

	// Remove overlay subtree first; a failure there aborts before masking.
	if inLayer {
		if err := u.layer.RemoveAll(name); err != nil {
			return err
		}
	}

	if baseExists || masked {
		if err := u.mask().add(name); err != nil {
			return err
		}
	}
	return nil
}

func (u *CopyOnWriteFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if isMetaPath(name) {
		if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
			return nil, syscall.EPERM
		}
		// Reading a same-named base entry remains possible.
		return u.base.OpenFile(name, flag, perm)
	}

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

		present, err := u.inLayer(name)
		if err != nil {
			return nil, err
		}
		if !present {
			if flag&os.O_CREATE == 0 {
				// Masked or never existing: behave like a missing file.
				return nil, notExistErr("open", name)
			}
			// Local (re)creation: ensure the parent exists in the
			// overlay so the new entry shadows only itself, not the other
			// hidden base descendants of a deleted directory.
			dir := filepath.Dir(name)
			baseParent, berr := IsDir(u.base, dir)
			if berr != nil && !os.IsNotExist(berr) {
				return nil, berr
			}
			layerParent, lerr := IsDir(u.layer, dir)
			if lerr != nil && !os.IsNotExist(lerr) {
				return nil, lerr
			}
			if baseParent || layerParent {
				if err = u.layer.MkdirAll(dir, 0o777); err != nil {
					return nil, err
				}
				return u.layer.OpenFile(name, flag, perm)
			}
			return nil, &os.PathError{
				Op:   "open",
				Path: name,
				Err:  syscall.ENOTDIR,
			}
		}

		return u.layer.OpenFile(name, flag, perm)
	}
	if b {
		return u.base.OpenFile(name, flag, perm)
	}
	present, err := u.inLayer(name)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, notExistErr("open", name)
	}
	return u.layer.OpenFile(name, flag, perm)
}

// mergeDirs returns a DirsMerger that filters whiteouted base entries and
// the hidden metadata area out of merged ReadDir results.
func (u *CopyOnWriteFs) mergeDirs(dir string) DirsMerger {
	return func(lofi, bofi []os.FileInfo) ([]os.FileInfo, error) {
		files := make(map[string]os.FileInfo)

		for _, fi := range lofi {
			if isMetaPath(filepath.Join(dir, fi.Name())) {
				continue
			}
			files[fi.Name()] = fi
		}

		for _, fi := range bofi {
			if _, exists := files[fi.Name()]; exists {
				continue
			}
			if u.mask().hiddenChild(dir, fi.Name()) {
				continue
			}
			files[fi.Name()] = fi
		}

		rfi := make([]os.FileInfo, 0, len(files))
		for _, fi := range files {
			rfi = append(rfi, fi)
		}
		return rfi, nil
	}
}

// This function handles the 9 different possibilities caused
// by the union which are the intersection of the following...
//
//	layer: doesn't exist, exists as a file, and exists as a directory
//	base:  doesn't exist, exists as a file, and exists as a directory
func (u *CopyOnWriteFs) Open(name string) (File, error) {
	if isMetaPath(name) {
		// Never expose overlay metadata; a same-named base file or
		// directory remains readable.
		return u.base.Open(name)
	}

	// Since the overlay overrides the base we check that first
	b, err := u.isBaseFile(name)
	if err != nil {
		return nil, err
	}

	// If overlay doesn't exist, return the base (base state irrelevant)
	if b {
		bf, err := u.base.Open(name)
		if err != nil {
			return nil, err
		}
		// Even a base-only directory listing must hide deleted descendants.
		if bfi, statErr := bf.Stat(); statErr == nil && bfi.IsDir() {
			return &maskedDirFile{File: bf, u: u, dir: name}, nil
		}
		return bf, nil
	}

	present, err := u.inLayer(name)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, notExistErr("open", name)
	}

	// If overlay is a file, return it (base state irrelevant)
	dir, err := IsDir(u.layer, name)
	if err != nil {
		return nil, err
	}
	if !dir {
		return u.layer.Open(name)
	}

	lfile, lErr := u.layer.Open(name)
	if lErr != nil {
		return nil, lErr
	}

	// Overlay is a directory, base state now matters.
	// Base state has 3 states to check but 2 outcomes:
	// A. It's a file or non-readable in the base (return just the overlay)
	// B. It's an accessible directory in the base (return a UnionFile)

	// If base is file or nonreadable, return overlay (still filtered).
	bdir, err := IsDir(u.base, name)
	if !bdir || err != nil {
		return &maskedDirFile{File: lfile, u: u, dir: name}, nil
	}

	// Both base & layer are directories
	// Return union file (if opens are without error)
	bfile, bErr := u.base.Open(name)
	if bErr != nil {
		lfile.Close()
		return nil, bErr
	}

	return &UnionFile{Base: bfile, Layer: lfile, Merger: u.mergeDirs(name)}, nil
}

func (u *CopyOnWriteFs) Mkdir(name string, perm os.FileMode) error {
	if isMetaPath(name) {
		return syscall.EPERM
	}
	present, err := u.inLayer(name)
	if err != nil {
		return err
	}
	if !present {
		if u.mask().hidden(name) {
			// Recreate the deleted directory locally in the overlay.
			return u.layer.MkdirAll(name, perm)
		}
		dir, err := IsDir(u.base, name)
		if err != nil {
			return u.layer.MkdirAll(name, perm)
		}
		if dir {
			return ErrFileExists
		}
		return u.layer.MkdirAll(name, perm)
	}
	return ErrFileExists
}

func (u *CopyOnWriteFs) Name() string {
	return "CopyOnWriteFs"
}

func (u *CopyOnWriteFs) MkdirAll(name string, perm os.FileMode) error {
	if isMetaPath(name) {
		return syscall.EPERM
	}
	present, err := u.inLayer(name)
	if err != nil {
		return err
	}
	if !present && !u.mask().hidden(name) {
		dir, err := IsDir(u.base, name)
		if err != nil {
			return u.layer.MkdirAll(name, perm)
		}
		if dir {
			// This is in line with how os.MkdirAll behaves.
			return nil
		}
	}
	return u.layer.MkdirAll(name, perm)
}

func (u *CopyOnWriteFs) Create(name string) (File, error) {
	return u.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o666)
}
