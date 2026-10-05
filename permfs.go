package afero

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// PermOp is a bitmask of operation categories granted by a PermRule.
type PermOp uint8

const (
	// PermRead allows opening and reading existing files and directories.
	PermRead PermOp = 1 << iota
	// PermWrite allows modifying the contents or metadata of existing files.
	PermWrite
	// PermCreate allows creating new files and directories.
	PermCreate
	// PermDelete allows removing files and directories, and is also required
	// on the source side of a rename.
	PermDelete
)

// PermRule grants the given operations on Path and everything below it.
// Paths are normalized with filepath.Clean before matching; the longest
// matching rule wins. A rule for the root ("/" or ".") matches everything.
type PermRule struct {
	Path string
	Ops  PermOp
}

// PermFs is an explicit permission wrapper around an Fs. Every operation is
// checked against the configured allow-list of PermRule entries; anything not
// explicitly granted is denied with os.ErrPermission.
//
// The check is not limited to Open: mutating calls on a returned File handle
// (Write, WriteAt, WriteString, Truncate) are checked against the permission
// profile captured when the handle was acquired, and Rename validates both
// the source and the target path before touching the underlying Fs, so a
// denied rename never creates the target file. A handle keeps the profile of
// the path it was acquired from, so renaming the file afterwards cannot
// change what the handle is allowed to do.
//
// PermFs only provides application-level authorization inside this process.
// It is not an OS-level sandbox and does not protect against code that
// bypasses the wrapper.
type PermFs struct {
	source Fs
	rules  []permRule
}

var _ Lstater = (*PermFs)(nil)

// NewPermFs wraps source with the given allow-list of rules. Operations that
// match no rule are denied.
func NewPermFs(source Fs, rules ...PermRule) *PermFs {
	p := &PermFs{source: source}
	for _, r := range rules {
		p.Allow(r.Path, r.Ops)
	}
	return p
}

// Allow adds (or replaces) the rule granting ops on path and its children.
func (p *PermFs) Allow(path string, ops PermOp) {
	path = normalizePermPath(path)
	for i, r := range p.rules {
		if r.path == path {
			p.rules[i].ops = ops
			return
		}
	}
	p.rules = append(p.rules, permRule{path: path, ops: ops})
}

// permRule is the normalized, internal form of PermRule.
type permRule struct {
	path string
	ops  PermOp
}

func normalizePermPath(name string) string {
	return filepath.Clean(name)
}

// matchPermPath reports whether the normalized rule path covers the
// normalized name: exact match, or the rule is a parent directory of name.
func matchPermPath(rule, name string) bool {
	if rule == name {
		return true
	}
	if rule == string(filepath.Separator) || rule == "." {
		return true
	}
	return strings.HasPrefix(name, rule+string(filepath.Separator))
}

// profile returns the operations granted for name by the longest matching
// rule, or 0 if no rule matches.
func (p *PermFs) profile(name string) PermOp {
	name = normalizePermPath(name)
	var ops PermOp
	best := -1
	for _, r := range p.rules {
		if len(r.path) > best && matchPermPath(r.path, name) {
			ops = r.ops
			best = len(r.path)
		}
	}
	return ops
}

func (p *PermFs) check(op, name string, need PermOp) error {
	if p.profile(name)&need != need {
		return &os.PathError{Op: op, Path: name, Err: os.ErrPermission}
	}
	return nil
}

// openFileFlagsNeed computes the permission categories implied by open flags.
// create is true when the target does not exist yet and O_CREATE is set.
func openFileFlagsNeed(flag int, create bool) PermOp {
	var need PermOp
	switch {
	case flag&os.O_RDWR != 0:
		need |= PermRead | PermWrite
	case flag&(os.O_WRONLY|os.O_APPEND|os.O_TRUNC) != 0:
		need |= PermWrite
	default:
		need |= PermRead
	}
	if create {
		need |= PermCreate
	}
	return need
}

func (p *PermFs) Name() string { return "PermFs" }

func (p *PermFs) Create(name string) (File, error) {
	if err := p.check("create", name, PermCreate); err != nil {
		return nil, err
	}
	f, err := p.source.Create(name)
	if err != nil {
		return nil, err
	}
	// A freshly created file is opened read-write by convention.
	ops := p.profile(name) & (PermRead | PermWrite)
	return newPermFile(f, name, ops, true), nil
}

func (p *PermFs) Mkdir(name string, perm os.FileMode) error {
	if err := p.check("mkdir", name, PermCreate); err != nil {
		return err
	}
	return p.source.Mkdir(name, perm)
}

func (p *PermFs) MkdirAll(path string, perm os.FileMode) error {
	if err := p.check("mkdir", path, PermCreate); err != nil {
		return err
	}
	return p.source.MkdirAll(path, perm)
}

func (p *PermFs) Open(name string) (File, error) {
	if err := p.check("open", name, PermRead); err != nil {
		return nil, err
	}
	f, err := p.source.Open(name)
	if err != nil {
		return nil, err
	}
	// Open yields a read-only handle; the captured profile reflects that.
	ops := p.profile(name) & PermRead
	return newPermFile(f, name, ops, true), nil
}

func (p *PermFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	// Decide whether this open would create the target, so a denied open
	// never creates the file as a side effect.
	_, statErr := p.source.Stat(name)
	create := flag&os.O_CREATE != 0 && os.IsNotExist(statErr)

	need := openFileFlagsNeed(flag, create)
	if err := p.check("openfile", name, need); err != nil {
		return nil, err
	}
	f, err := p.source.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	// Pin the profile at acquisition: the handle keeps these rights even if
	// the file is later renamed to a path with a different profile.
	ops := p.profile(name) & openFileFlagsNeed(flag&^os.O_CREATE, false)
	return newPermFile(f, name, ops, true), nil
}

func (p *PermFs) Remove(name string) error {
	if err := p.check("remove", name, PermDelete); err != nil {
		return err
	}
	return p.source.Remove(name)
}

func (p *PermFs) RemoveAll(path string) error {
	if err := p.check("remove_all", path, PermDelete); err != nil {
		return err
	}
	return p.source.RemoveAll(path)
}

func (p *PermFs) Rename(oldname, newname string) error {
	// Both ends are authorized before the underlying rename runs, so a
	// denied rename leaves no target file behind.
	if err := p.check("rename", oldname, PermDelete); err != nil {
		return err
	}
	if err := p.check("rename", newname, PermCreate); err != nil {
		return err
	}
	return p.source.Rename(oldname, newname)
}

func (p *PermFs) Stat(name string) (os.FileInfo, error) {
	// Metadata reads are always allowed; content access is gated by Open.
	return p.source.Stat(name)
}

func (p *PermFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	if lstater, ok := p.source.(Lstater); ok {
		return lstater.LstatIfPossible(name)
	}
	fi, err := p.source.Stat(name)
	return fi, false, err
}

func (p *PermFs) Chmod(name string, mode os.FileMode) error {
	if err := p.check("chmod", name, PermWrite); err != nil {
		return err
	}
	return p.source.Chmod(name, mode)
}

func (p *PermFs) Chown(name string, uid, gid int) error {
	if err := p.check("chown", name, PermWrite); err != nil {
		return err
	}
	return p.source.Chown(name, uid, gid)
}

func (p *PermFs) Chtimes(name string, atime, mtime time.Time) error {
	if err := p.check("chtimes", name, PermWrite); err != nil {
		return err
	}
	return p.source.Chtimes(name, atime, mtime)
}

func (p *PermFs) SymlinkIfPossible(oldname, newname string) error {
	if err := p.check("symlink", newname, PermCreate); err != nil {
		return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: os.ErrPermission}
	}
	if linker, ok := p.source.(Linker); ok {
		return linker.SymlinkIfPossible(oldname, newname)
	}
	return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: ErrNoSymlink}
}

func (p *PermFs) ReadlinkIfPossible(name string) (string, error) {
	if err := p.check("readlink", name, PermRead); err != nil {
		return "", err
	}
	if reader, ok := p.source.(LinkReader); ok {
		return reader.ReadlinkIfPossible(name)
	}
	return "", &os.PathError{Op: "readlink", Path: name, Err: ErrNoReadlink}
}

// PermFile is a File handle whose permitted operations were captured when
// the handle was acquired. The profile is pinned to the origin path: later
// renames of the underlying file neither widen nor narrow it.
type PermFile struct {
	f      File
	origin string
	ops    PermOp
	// closeUnderlying decides whether Close closes the wrapped file.
	// Handles obtained from a PermFs own their file (true); handles
	// borrowed via WrapPermFile choose explicitly.
	closeUnderlying bool
}

var _ File = (*PermFile)(nil)

func newPermFile(f File, origin string, ops PermOp, closeUnderlying bool) *PermFile {
	return &PermFile{
		f:               f,
		origin:          normalizePermPath(origin),
		ops:             ops,
		closeUnderlying: closeUnderlying,
	}
}

// WrapPermFile wraps a borrowed file handle with an explicit permission
// profile. origin is recorded as the handle provenance. If ownClose is
// false, Close on the returned handle is a no-op and the caller keeps
// ownership of f; if true, Close closes f.
func WrapPermFile(f File, origin string, ops PermOp, ownClose bool) *PermFile {
	return newPermFile(f, origin, ops, ownClose)
}

// Origin returns the normalized path the handle was acquired with.
func (f *PermFile) Origin() string { return f.origin }

// Profile returns the operations this handle is permitted to perform.
func (f *PermFile) Profile() PermOp { return f.ops }

func (f *PermFile) permitted(op string, need PermOp) error {
	if f.ops&need != need {
		return &os.PathError{Op: op, Path: f.origin, Err: os.ErrPermission}
	}
	return nil
}

// Close closes the underlying file only if this handle owns it; borrowed
// handles (WrapPermFile with ownClose=false) leave the file open.
func (f *PermFile) Close() error {
	if !f.closeUnderlying {
		return nil
	}
	return f.f.Close()
}

func (f *PermFile) Read(b []byte) (int, error) {
	if err := f.permitted("read", PermRead); err != nil {
		return 0, err
	}
	return f.f.Read(b)
}

func (f *PermFile) ReadAt(b []byte, off int64) (int, error) {
	if err := f.permitted("read", PermRead); err != nil {
		return 0, err
	}
	return f.f.ReadAt(b, off)
}

func (f *PermFile) Seek(offset int64, whence int) (int64, error) {
	return f.f.Seek(offset, whence)
}

func (f *PermFile) Write(b []byte) (int, error) {
	if err := f.permitted("write", PermWrite); err != nil {
		return 0, err
	}
	return f.f.Write(b)
}

func (f *PermFile) WriteAt(b []byte, off int64) (int, error) {
	if err := f.permitted("write", PermWrite); err != nil {
		return 0, err
	}
	return f.f.WriteAt(b, off)
}

func (f *PermFile) WriteString(s string) (int, error) {
	if err := f.permitted("write", PermWrite); err != nil {
		return 0, err
	}
	return f.f.WriteString(s)
}

func (f *PermFile) Truncate(size int64) error {
	if err := f.permitted("truncate", PermWrite); err != nil {
		return err
	}
	return f.f.Truncate(size)
}

func (f *PermFile) Name() string { return f.f.Name() }

func (f *PermFile) Readdir(count int) ([]os.FileInfo, error) {
	if err := f.permitted("readdir", PermRead); err != nil {
		return nil, err
	}
	return f.f.Readdir(count)
}

func (f *PermFile) Readdirnames(n int) ([]string, error) {
	if err := f.permitted("readdir", PermRead); err != nil {
		return nil, err
	}
	return f.f.Readdirnames(n)
}

func (f *PermFile) Stat() (os.FileInfo, error) { return f.f.Stat() }

func (f *PermFile) Sync() error { return f.f.Sync() }
