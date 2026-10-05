package afero

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Perm is a bitmask of operation categories that can be granted on a path.
type Perm uint32

const (
	// PermRead allows opening files for reading and reading file contents
	// or metadata.
	PermRead Perm = 1 << iota
	// PermWrite allows opening files for writing and mutating file contents
	// or metadata.
	PermWrite
	// PermCreate allows creating new files and directories.
	PermCreate
	// PermDelete allows removing files and directories and is required to
	// move a file out of a path via Rename.
	PermDelete
)

// PermAll is the union of every Perm bit.
const PermAll = PermRead | PermWrite | PermCreate | PermDelete

// PermFs is an explicit application-level authorization wrapper around
// another Fs.
//
// Access is granted by canonical path and operation category: a request is
// allowed only when an explicit rule grants the requested Perm on the
// cleaned path. The default policy is deny-all. Checks are performed both
// on the Fs operations and on the file handles they return: a handle keeps
// the profile captured when it was issued, so later Write, Truncate or
// Rename calls cannot widen it, and renaming a path does not bypass the
// grant on the original path.
//
// This is purely an application-level authorization check. It does not
// provide or claim any OS sandbox.
type PermFs struct {
	source Fs
	mu     sync.RWMutex
	rules  []permRule
}

type permRule struct {
	path  string
	allow Perm
	deny  Perm
}

// NewPermFs wraps source with an empty, deny-all permission policy.
func NewPermFs(source Fs) *PermFs {
	return &PermFs{source: source}
}

// Allow grants the given operation categories on path (and every path
// beneath it). Rules are matched on the canonical (cleaned) path.
func (p *PermFs) Allow(path string, perms Perm) *PermFs {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, permRule{path: canonicalPermPath(path), allow: perms})
	return p
}

// Deny revokes the given operation categories on path (and every path
// beneath it). Deny rules always take precedence over Allow rules.
func (p *PermFs) Deny(path string, perms Perm) *PermFs {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, permRule{path: canonicalPermPath(path), deny: perms})
	return p
}

// Source returns the wrapped filesystem.
func (p *PermFs) Source() Fs { return p.source }

// Name returns the name of this filesystem.
func (p *PermFs) Name() string { return "PermissionFs" }

func canonicalPermPath(name string) string {
	name = filepath.ToSlash(filepath.Clean(name))
	if name == "." {
		return "/"
	}
	return name
}

// permPathMatch reports whether rulePath covers name, i.e. they are equal
// or name is located directly beneath rulePath.
func permPathMatch(rulePath, name string) bool {
	if rulePath == "/" {
		return true
	}
	return name == rulePath || strings.HasPrefix(name, rulePath+"/")
}

// allows reports whether perms are granted on the canonical name. The
// default policy is deny-all: at least one Allow rule must grant every
// requested bit and no Deny rule may revoke any of them.
func (p *PermFs) allows(name string, perms Perm) bool {
	name = canonicalPermPath(name)
	p.mu.RLock()
	defer p.mu.RUnlock()
	var allow, deny Perm
	for _, r := range p.rules {
		if permPathMatch(r.path, name) {
			allow |= r.allow
			deny |= r.deny
		}
	}
	return deny&perms == 0 && allow&perms == perms
}

func (p *PermFs) check(op, name string, perms Perm) error {
	if p.allows(name, perms) {
		return nil
	}
	return &os.PathError{Op: op, Path: name, Err: syscall.EACCES}
}

func (p *PermFs) newFile(f File, name string, allowed Perm) *PermFile {
	return &PermFile{f: f, name: canonicalPermPath(name), allowed: allowed}
}

// Create creates a file only if PermCreate is granted on the path. The
// returned handle carries read/write permissions for the new file.
func (p *PermFs) Create(name string) (File, error) {
	if err := p.check("create", name, PermCreate); err != nil {
		return nil, err
	}
	f, err := p.source.Create(name)
	if err != nil {
		return nil, err
	}
	return p.newFile(f, name, PermRead|PermWrite), nil
}

// Open opens a file only if PermRead is granted on the path. The returned
// handle carries only read permissions.
func (p *PermFs) Open(name string) (File, error) {
	if err := p.check("open", name, PermRead); err != nil {
		return nil, err
	}
	f, err := p.source.Open(name)
	if err != nil {
		return nil, err
	}
	return p.newFile(f, name, PermRead), nil
}

// OpenFile opens a file only if every required operation category is
// granted. Write flags require PermWrite, O_TRUNC requires PermWrite as
// well, and O_CREATE requires PermCreate when the file does not already
// exist. Denied requests fail before the source filesystem is touched, so
// no target file is created or truncated.
func (p *PermFs) OpenFile(name string, flag int, mode os.FileMode) (File, error) {
	need := Perm(0)
	if flag&os.O_WRONLY == 0 {
		need |= PermRead
	}
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_TRUNC) != 0 {
		need |= PermWrite
	}
	if flag&os.O_CREATE != 0 {
		switch _, err := p.source.Stat(name); {
		case err == nil:
			// The file already exists; O_CREATE does not create anything.
		case os.IsNotExist(err):
			need |= PermCreate
		default:
			return nil, err
		}
	}
	if err := p.check("open", name, need); err != nil {
		return nil, err
	}
	f, err := p.source.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	allowed := Perm(0)
	if need&PermRead != 0 {
		allowed |= PermRead
	}
	if need&PermWrite != 0 {
		allowed |= PermWrite
	}
	return p.newFile(f, name, allowed), nil
}

// Remove removes a file only if PermDelete is granted on the path.
func (p *PermFs) Remove(name string) error {
	if err := p.check("remove", name, PermDelete); err != nil {
		return err
	}
	return p.source.Remove(name)
}

// RemoveAll removes a path only if PermDelete is granted on it.
func (p *PermFs) RemoveAll(name string) error {
	if err := p.check("removeall", name, PermDelete); err != nil {
		return err
	}
	return p.source.RemoveAll(name)
}

// Rename moves a file only if PermDelete is granted on the old path and
// PermCreate is granted on the new path: a rename both removes the file
// from its original location and creates it at the destination, so the
// grant on the original path cannot be bypassed by a path change. Denied
// requests fail before the source filesystem is touched, so no target file
// is created.
func (p *PermFs) Rename(oldname, newname string) error {
	if err := p.check("rename", oldname, PermDelete); err != nil {
		return err
	}
	if err := p.check("rename", newname, PermCreate); err != nil {
		return err
	}
	return p.source.Rename(oldname, newname)
}

// Stat returns file metadata only if PermRead is granted on the path.
func (p *PermFs) Stat(name string) (os.FileInfo, error) {
	if err := p.check("stat", name, PermRead); err != nil {
		return nil, err
	}
	return p.source.Stat(name)
}

// Mkdir creates a directory only if PermCreate is granted on the path.
func (p *PermFs) Mkdir(name string, mode os.FileMode) error {
	if err := p.check("mkdir", name, PermCreate); err != nil {
		return err
	}
	return p.source.Mkdir(name, mode)
}

// MkdirAll creates a directory tree only if PermCreate is granted on the
// path. An existing directory is a no-op and does not require create
// permission.
func (p *PermFs) MkdirAll(name string, mode os.FileMode) error {
	if p.allows(name, PermCreate) {
		return p.source.MkdirAll(name, mode)
	}
	if fi, err := p.source.Stat(name); err == nil && fi.IsDir() {
		return nil
	}
	return p.check("mkdir", name, PermCreate)
}

// Chmod changes a file mode only if PermWrite is granted on the path.
func (p *PermFs) Chmod(name string, mode os.FileMode) error {
	if err := p.check("chmod", name, PermWrite); err != nil {
		return err
	}
	return p.source.Chmod(name, mode)
}

// Chown changes a file owner only if PermWrite is granted on the path.
func (p *PermFs) Chown(name string, uid, gid int) error {
	if err := p.check("chown", name, PermWrite); err != nil {
		return err
	}
	return p.source.Chown(name, uid, gid)
}

// Chtimes changes a file timestamps only if PermWrite is granted on the
// path.
func (p *PermFs) Chtimes(name string, atime, mtime time.Time) error {
	if err := p.check("chtimes", name, PermWrite); err != nil {
		return err
	}
	return p.source.Chtimes(name, atime, mtime)
}

// PermFile is a File handle returned by PermFs. It wraps a handle borrowed
// from the source filesystem and enforces, on every operation, the
// permission profile captured when the handle was issued. The captured
// name and profile never change, so renaming the underlying file cannot
// widen or bypass the original authorization.
//
// Close policy: the borrowed source handle is closed exactly once, when
// this handle is closed; closing is idempotent and returns ErrFileClosed
// on subsequent calls. The source filesystem itself is never closed.
type PermFile struct {
	f       File // borrowed source handle
	name    string
	allowed Perm // profile captured at issuance
	closed  bool
	mu      sync.Mutex
}

// Source returns the borrowed underlying file handle, preserving the
// handle's origin.
func (f *PermFile) Source() File { return f.f }

// Name returns the canonical name captured when the handle was issued.
func (f *PermFile) Name() string { return f.name }

// Close closes the borrowed source handle exactly once.
func (f *PermFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrFileClosed
	}
	f.closed = true
	return f.f.Close()
}

func (f *PermFile) check(op string, perms Perm) error {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return ErrFileClosed
	}
	if f.allowed&perms == perms {
		return nil
	}
	return &os.PathError{Op: op, Path: f.name, Err: syscall.EACCES}
}

// Read reads from the file, requiring PermRead on the captured profile.
func (f *PermFile) Read(b []byte) (int, error) {
	if err := f.check("read", PermRead); err != nil {
		return 0, err
	}
	return f.f.Read(b)
}

// ReadAt reads from the file at the given offset, requiring PermRead.
func (f *PermFile) ReadAt(b []byte, off int64) (int, error) {
	if err := f.check("read", PermRead); err != nil {
		return 0, err
	}
	return f.f.ReadAt(b, off)
}

// Seek moves the file offset, requiring PermRead.
func (f *PermFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.check("seek", PermRead); err != nil {
		return 0, err
	}
	return f.f.Seek(offset, whence)
}

// Write writes to the file, requiring PermWrite on the captured profile.
func (f *PermFile) Write(b []byte) (int, error) {
	if err := f.check("write", PermWrite); err != nil {
		return 0, err
	}
	return f.f.Write(b)
}

// WriteAt writes to the file at the given offset, requiring PermWrite.
func (f *PermFile) WriteAt(b []byte, off int64) (int, error) {
	if err := f.check("write", PermWrite); err != nil {
		return 0, err
	}
	return f.f.WriteAt(b, off)
}

// WriteString writes a string to the file, requiring PermWrite.
func (f *PermFile) WriteString(s string) (int, error) {
	if err := f.check("write", PermWrite); err != nil {
		return 0, err
	}
	return f.f.WriteString(s)
}

// Truncate changes the file size, requiring PermWrite on the captured
// profile.
func (f *PermFile) Truncate(size int64) error {
	if err := f.check("truncate", PermWrite); err != nil {
		return err
	}
	return f.f.Truncate(size)
}

// Sync flushes the file, requiring PermWrite.
func (f *PermFile) Sync() error {
	if err := f.check("sync", PermWrite); err != nil {
		return err
	}
	return f.f.Sync()
}

// Stat returns file metadata, requiring PermRead.
func (f *PermFile) Stat() (os.FileInfo, error) {
	if err := f.check("stat", PermRead); err != nil {
		return nil, err
	}
	return f.f.Stat()
}

// Readdir reads directory entries, requiring PermRead.
func (f *PermFile) Readdir(count int) ([]os.FileInfo, error) {
	if err := f.check("readdir", PermRead); err != nil {
		return nil, err
	}
	return f.f.Readdir(count)
}

// Readdirnames reads directory entry names, requiring PermRead.
func (f *PermFile) Readdirnames(count int) ([]string, error) {
	if err := f.check("readdir", PermRead); err != nil {
		return nil, err
	}
	return f.f.Readdirnames(count)
}
