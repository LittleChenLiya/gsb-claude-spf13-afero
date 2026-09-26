package afero

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// InjectOp identifies the kind of filesystem operation a FaultRule targets.
type InjectOp string

const (
	// InjectOpen matches calls to Open, OpenFile and (because it is implemented
	// on top of OpenFile) Create.
	InjectOpen InjectOp = "open"
	// InjectRead matches calls to File.Read.
	InjectRead InjectOp = "read"
	// InjectWrite matches calls to File.Write and File.WriteString.
	InjectWrite InjectOp = "write"
	// InjectRename matches calls to Rename.
	InjectRename InjectOp = "rename"
	// InjectClose matches calls to File.Close.
	InjectClose InjectOp = "close"
)

// FaultRule describes a single injected failure.
//
// Rules are evaluated against the cleaned path and the operation kind in
// declaration order; only the first matching rule is considered.
//
// A rule fires on its Nth matching call (N is 1-based, so N <= 1 means the
// first call). When Repeat is false the rule fires at most once. When Repeat
// is true it fires on the Nth and every subsequent matching call.
//
// For write faults WritePrefix may be greater than zero: that many leading
// bytes of the write payload are forwarded to the underlying filesystem
// (clamped to the payload length) before Err is returned, producing a real
// partial write as required by the io.Writer contract.
//
// OnFired, when non-nil, is invoked after a rule fires. It is never invoked
// while the wrapper's internal lock is held, so it is safe for it to call
// back into the wrapper.
type FaultRule struct {
	Op          InjectOp
	Path        string
	N           int
	Repeat      bool
	WritePrefix int
	Err         error
	OnFired     func(rule FaultRule)
}

var _ Fs = (*FaultInjectingFs)(nil)

// FaultInjectingFs wraps an Fs and injects failures according to its rules.
//
// Call counters are isolated per wrapper instance and per (operation, path)
// pair. Incrementing a counter is atomic, so the observed call numbers are
// reliable under concurrent use. Files opened through the wrapper stay
// attached to the same wrapper: operations performed on them count toward
// the wrapper that opened them.
type FaultInjectingFs struct {
	source Fs

	mu      sync.Mutex
	enabled bool
	rules   []FaultRule
	calls   map[injectKey]int
}

type injectKey struct {
	op   InjectOp
	path string
}

// NewFaultInjectingFs wraps source with the given rules. Fault injection is
// enabled by default.
func NewFaultInjectingFs(source Fs, rules ...FaultRule) *FaultInjectingFs {
	fs := &FaultInjectingFs{
		source:  source,
		enabled: true,
		rules:   append([]FaultRule(nil), rules...),
		calls:   make(map[injectKey]int),
	}
	return fs
}

// SetRules replaces the fault rules. Existing call counters are preserved.
func (f *FaultInjectingFs) SetRules(rules ...FaultRule) {
	f.mu.Lock()
	f.rules = append([]FaultRule(nil), rules...)
	f.mu.Unlock()
}

// SetEnabled turns fault injection on or off. While disabled, the wrapper
// behaves exactly like the underlying Fs and calls are not counted. Call
// counters are preserved across disable/enable cycles.
func (f *FaultInjectingFs) SetEnabled(enabled bool) {
	f.mu.Lock()
	f.enabled = enabled
	f.mu.Unlock()
}

// Enabled reports whether fault injection is active.
func (f *FaultInjectingFs) Enabled() bool {
	f.mu.Lock()
	enabled := f.enabled
	f.mu.Unlock()
	return enabled
}

// Calls returns the number of times op has been invoked on path through this
// wrapper. Path is cleaned before lookup.
func (f *FaultInjectingFs) Calls(op InjectOp, path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[injectKey{op: op, path: filepath.Clean(path)}]
}

// match returns a copy of the first rule that fires for op/path together with
// whether such a rule exists. It increments and reads the call counter while
// holding the lock; the rule copy is returned so the caller can invoke the
// rule callback (or inspect it) without holding the lock.
func (f *FaultInjectingFs) match(op InjectOp, path string) (FaultRule, bool) {
	key := injectKey{op: op, path: filepath.Clean(path)}

	f.mu.Lock()
	if !f.enabled {
		f.mu.Unlock()
		return FaultRule{}, false
	}
	n := f.calls[key] + 1
	f.calls[key] = n

	for i := range f.rules {
		rule := f.rules[i]
		if rule.Op != op || filepath.Clean(rule.Path) != key.path {
			continue
		}
		if n < effectiveN(rule) {
			continue
		}
		if !rule.Repeat && n > effectiveN(rule) {
			continue
		}
		f.mu.Unlock()
		return rule, true
	}
	f.mu.Unlock()
	return FaultRule{}, false
}

func effectiveN(rule FaultRule) int {
	if rule.N < 1 {
		return 1
	}
	return rule.N
}

func (f *FaultInjectingFs) Name() string { return "FaultInjectingFs" }

func (f *FaultInjectingFs) Create(name string) (File, error) {
	return f.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

func (f *FaultInjectingFs) Open(name string) (File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

func (f *FaultInjectingFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if rule, ok := f.match(InjectOpen, name); ok {
		defer fire(rule)
		return nil, rule.Err
	}
	file, err := f.source.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultInjectingFile{File: file, fs: f, path: filepath.Clean(name)}, nil
}

func (f *FaultInjectingFs) Rename(oldname, newname string) error {
	if rule, ok := f.match(InjectRename, oldname); ok {
		defer fire(rule)
		return rule.Err
	}
	return f.source.Rename(oldname, newname)
}

func (f *FaultInjectingFs) Stat(name string) (os.FileInfo, error) {
	return f.source.Stat(name)
}

func (f *FaultInjectingFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	if lsf, ok := f.source.(Lstater); ok {
		return lsf.LstatIfPossible(name)
	}
	fi, err := f.Stat(name)
	return fi, false, err
}

func (f *FaultInjectingFs) Mkdir(name string, perm os.FileMode) error {
	return f.source.Mkdir(name, perm)
}

func (f *FaultInjectingFs) MkdirAll(path string, perm os.FileMode) error {
	return f.source.MkdirAll(path, perm)
}

func (f *FaultInjectingFs) Remove(name string) error {
	return f.source.Remove(name)
}

func (f *FaultInjectingFs) RemoveAll(path string) error {
	return f.source.RemoveAll(path)
}

func (f *FaultInjectingFs) Chmod(name string, mode os.FileMode) error {
	return f.source.Chmod(name, mode)
}

func (f *FaultInjectingFs) Chown(name string, uid, gid int) error {
	return f.source.Chown(name, uid, gid)
}

func (f *FaultInjectingFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return f.source.Chtimes(name, atime, mtime)
}

// fire invokes a rule callback outside the wrapper's lock.
func fire(rule FaultRule) {
	if rule.OnFired != nil {
		rule.OnFired(rule)
	}
}

var _ File = (*faultInjectingFile)(nil)

type faultInjectingFile struct {
	File
	fs   *FaultInjectingFs
	path string
}

func (f *faultInjectingFile) Name() string { return f.path }

func (f *faultInjectingFile) Read(p []byte) (int, error) {
	if rule, ok := f.fs.match(InjectRead, f.path); ok {
		defer fire(rule)
		return 0, rule.Err
	}
	return f.File.Read(p)
}

func (f *faultInjectingFile) Write(p []byte) (int, error) {
	rule, ok := f.fs.match(InjectWrite, f.path)
	if !ok {
		return f.File.Write(p)
	}
	defer fire(rule)

	if rule.WritePrefix <= 0 {
		return 0, rule.Err
	}
	prefix := rule.WritePrefix
	if prefix > len(p) {
		prefix = len(p)
	}
	n, werr := f.File.Write(p[:prefix])
	if werr != nil {
		return n, errors.Join(werr, rule.Err)
	}
	return n, rule.Err
}

func (f *faultInjectingFile) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

func (f *faultInjectingFile) Close() error {
	rule, fired := f.fs.match(InjectClose, f.path)
	err := f.File.Close()
	if fired {
		defer fire(rule)
		if err != nil {
			return errors.Join(err, rule.Err)
		}
		return rule.Err
	}
	return err
}
