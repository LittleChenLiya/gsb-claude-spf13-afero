// Copyright © 2026 Steve Francia <spf@spf13.com>.
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

package afero

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// FaultOp identifies the kind of operation a FaultRule applies to.
type FaultOp int

const (
	// FaultOpOpen matches Open and OpenFile calls on the Fs.
	FaultOpOpen FaultOp = iota
	// FaultOpRead matches Read calls on files opened through the FaultFs.
	FaultOpRead
	// FaultOpWrite matches Write calls on files opened through the FaultFs.
	FaultOpWrite
	// FaultOpRename matches Rename calls on the Fs. The rule path is
	// matched against the (cleaned) source name of the rename.
	FaultOpRename
	// FaultOpClose matches Close calls on files opened through the FaultFs.
	FaultOpClose
)

func (op FaultOp) String() string {
	switch op {
	case FaultOpOpen:
		return "open"
	case FaultOpRead:
		return "read"
	case FaultOpWrite:
		return "write"
	case FaultOpRename:
		return "rename"
	case FaultOpClose:
		return "close"
	default:
		return "unknown"
	}
}

// FaultRule describes a single fault injection rule.
//
// Rules are matched against the cleaned operation path, the operation kind
// and the 1-based sequence number of the call. Only the first matching rule
// (in declaration order) is used for a call.
type FaultRule struct {
	// Op is the operation kind this rule applies to.
	Op FaultOp
	// Path is matched against the cleaned path of the operation.
	Path string
	// N is the 1-based call number (per cleaned path and operation) that
	// triggers the rule. Values below 1 are treated as 1.
	N int
	// Err is the error returned when the rule fires. It is wrapped with
	// context using %w, so errors.Is(err, Err) keeps working. If Err is
	// nil the rule only runs Callback and the operation proceeds normally.
	Err error
	// WritePrefix only applies to FaultOpWrite: before returning Err, this
	// many bytes of the caller's buffer are written to the underlying file,
	// producing a real short write. Values larger than the buffer are
	// clamped to the buffer length.
	WritePrefix int
	// Repeat controls whether the rule keeps firing after the Nth call.
	// If false (the default) the rule fires exactly once, on the Nth call.
	// If true it fires on the Nth call and on every subsequent matching call.
	Repeat bool
	// Callback, if set, is invoked every time the rule fires. It runs
	// outside of the FaultFs internal locks, so it may safely call back
	// into the same FaultFs.
	Callback func(FaultInfo)
}

// FaultInfo describes a fired fault injection rule.
type FaultInfo struct {
	// Op is the operation kind of the call.
	Op FaultOp
	// Path is the cleaned path of the call.
	Path string
	// Call is the 1-based sequence number of the call for this
	// (operation, path) pair.
	Call int
	// Rule is the rule that fired.
	Rule FaultRule
}

type faultKey struct {
	op   FaultOp
	path string
}

type compiledFaultRule struct {
	FaultRule
	cleanPath string
}

// FaultFs is an Fs wrapper that injects controllable faults, useful for
// exercising error paths without, for example, actually filling a disk.
//
// Call sequence numbers are tracked per FaultFs instance, keyed by
// (operation, cleaned path), and are safe for concurrent use. Files opened
// through a FaultFs keep counting Read, Write and Close calls through the
// same FaultFs instance.
type FaultFs struct {
	source  Fs
	rules   []compiledFaultRule
	enabled atomic.Bool

	mu     sync.Mutex
	counts map[faultKey]int
}

var _ Fs = (*FaultFs)(nil)

// NewFaultFs wraps source with fault injection according to rules. Fault
// injection is enabled by default; use SetEnabled to toggle it.
func NewFaultFs(source Fs, rules ...FaultRule) *FaultFs {
	f := &FaultFs{
		source: source,
		counts: make(map[faultKey]int),
	}
	for _, r := range rules {
		if r.N < 1 {
			r.N = 1
		}
		f.rules = append(f.rules, compiledFaultRule{
			FaultRule: r,
			cleanPath: filepath.Clean(r.Path),
		})
	}
	f.enabled.Store(true)
	return f
}

// SetEnabled toggles fault injection. While disabled the FaultFs behaves
// exactly like its underlying Fs and no calls are counted.
func (f *FaultFs) SetEnabled(enabled bool) {
	f.enabled.Store(enabled)
}

// Enabled reports whether fault injection is currently active.
func (f *FaultFs) Enabled() bool {
	return f.enabled.Load()
}

// CallCount returns how many calls for the given operation and path have
// been counted so far. The path is cleaned before lookup.
func (f *FaultFs) CallCount(op FaultOp, path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[faultKey{op: op, path: filepath.Clean(path)}]
}

// decide counts the call and returns the first matching rule, if any. The
// internal lock is only held to update the counters; rule matching and any
// callback execution happen after it has been released.
func (f *FaultFs) decide(op FaultOp, path string) (FaultInfo, bool) {
	if !f.enabled.Load() {
		return FaultInfo{}, false
	}
	clean := filepath.Clean(path)
	key := faultKey{op: op, path: clean}

	f.mu.Lock()
	f.counts[key]++
	call := f.counts[key]
	f.mu.Unlock()

	for i := range f.rules {
		r := &f.rules[i]
		if r.Op == op && r.cleanPath == clean && (call == r.N || (r.Repeat && call >= r.N)) {
			return FaultInfo{Op: op, Path: clean, Call: call, Rule: r.FaultRule}, true
		}
	}
	return FaultInfo{}, false
}

// observe runs the rule callback, if any. It is never called while holding
// the FaultFs internal lock.
func (f *FaultFs) observe(info FaultInfo) {
	if info.Rule.Callback != nil {
		info.Rule.Callback(info)
	}
}

// errFor returns the wrapped error for a fired rule, or nil if the rule
// carries no error.
func (f *FaultFs) errFor(info FaultInfo) error {
	if info.Rule.Err == nil {
		return nil
	}
	return fmt.Errorf("faultfs: injected %s failure on %q (call %d): %w",
		info.Op, info.Path, info.Call, info.Rule.Err)
}

func (f *FaultFs) wrapFile(file File, name string) File {
	return &faultFile{File: file, fs: f, name: filepath.Clean(name)}
}

// Create creates a file in the underlying filesystem. Create itself is not
// counted, but the returned file is wrapped so Write and Close rules apply.
func (f *FaultFs) Create(name string) (File, error) {
	file, err := f.source.Create(name)
	if err != nil {
		return nil, err
	}
	return f.wrapFile(file, name), nil
}

func (f *FaultFs) Mkdir(name string, perm os.FileMode) error {
	return f.source.Mkdir(name, perm)
}

func (f *FaultFs) MkdirAll(path string, perm os.FileMode) error {
	return f.source.MkdirAll(path, perm)
}

func (f *FaultFs) Open(name string) (File, error) {
	if info, ok := f.decide(FaultOpOpen, name); ok {
		f.observe(info)
		if err := f.errFor(info); err != nil {
			return nil, err
		}
	}
	file, err := f.source.Open(name)
	if err != nil {
		return nil, err
	}
	return f.wrapFile(file, name), nil
}

func (f *FaultFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if info, ok := f.decide(FaultOpOpen, name); ok {
		f.observe(info)
		if err := f.errFor(info); err != nil {
			return nil, err
		}
	}
	file, err := f.source.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f.wrapFile(file, name), nil
}

func (f *FaultFs) Remove(name string) error {
	return f.source.Remove(name)
}

func (f *FaultFs) RemoveAll(path string) error {
	return f.source.RemoveAll(path)
}

// Rename renames a file. FaultOpRename rules are matched and counted
// against the cleaned oldname.
func (f *FaultFs) Rename(oldname, newname string) error {
	if info, ok := f.decide(FaultOpRename, oldname); ok {
		f.observe(info)
		if err := f.errFor(info); err != nil {
			return err
		}
	}
	return f.source.Rename(oldname, newname)
}

func (f *FaultFs) Stat(name string) (os.FileInfo, error) {
	return f.source.Stat(name)
}

func (f *FaultFs) Name() string {
	return "FaultFs"
}

func (f *FaultFs) Chmod(name string, mode os.FileMode) error {
	return f.source.Chmod(name, mode)
}

func (f *FaultFs) Chown(name string, uid, gid int) error {
	return f.source.Chown(name, uid, gid)
}

func (f *FaultFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return f.source.Chtimes(name, atime, mtime)
}

// faultFile wraps a File opened through a FaultFs so that Read, Write and
// Close calls keep being counted (and possibly failed) by the same FaultFs.
type faultFile struct {
	File
	fs   *FaultFs
	name string
}

func (f *faultFile) Read(p []byte) (int, error) {
	if info, ok := f.fs.decide(FaultOpRead, f.name); ok {
		f.fs.observe(info)
		if err := f.fs.errFor(info); err != nil {
			return 0, err
		}
	}
	return f.File.Read(p)
}

func (f *faultFile) Write(p []byte) (int, error) {
	if info, ok := f.fs.decide(FaultOpWrite, f.name); ok {
		f.fs.observe(info)
		if err := f.fs.errFor(info); err != nil {
			n := 0
			if prefix := info.Rule.WritePrefix; prefix > 0 {
				if prefix > len(p) {
					prefix = len(p)
				}
				n, _ = f.File.Write(p[:prefix])
			}
			return n, err
		}
	}
	return f.File.Write(p)
}

// WriteString routes through Write so that FaultOpWrite rules and counters
// apply to it as well.
func (f *faultFile) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

func (f *faultFile) Close() error {
	if info, ok := f.fs.decide(FaultOpClose, f.name); ok {
		f.fs.observe(info)
		if err := f.fs.errFor(info); err != nil {
			return err
		}
	}
	return f.File.Close()
}
