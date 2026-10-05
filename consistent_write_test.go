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
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// failWriteFile wraps a File and fails the failAt-th WriteAt call.
type failWriteFile struct {
	File
	count  int
	failAt int
}

func (f *failWriteFile) WriteAt(p []byte, off int64) (int, error) {
	f.count++
	if f.count == f.failAt {
		return 0, errors.New("injected write failure")
	}
	return f.File.WriteAt(p, off)
}

// TestConsistentWritePastEnd verifies the six-byte example: after writing
// "abc", seeking to 5 and writing "X", the file reads back as "abc" plus
// two zero bytes plus "X". This runs against both the in-memory and the
// native small-file backends.
func TestConsistentWritePastEnd(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source)
			name := filepath.Join(t.TempDir(), "file")
			if err := WriteFile(cfs, name, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := cfs.OpenFile(name, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if _, err := f.Seek(5, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			n, err := f.Write([]byte("X"))
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Errorf("write n = %d; want 1", n)
			}

			info, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != 6 {
				t.Errorf("size = %d; want 6", info.Size())
			}

			content, err := ReadFile(cfs, name)
			if err != nil {
				t.Fatal(err)
			}
			want := "abc\x00\x00X"
			if string(content) != want {
				t.Errorf("content = %q; want %q", content, want)
			}
		})
	}
}

// TestConsistentWriteAtPastEnd verifies the same six-byte example using
// WriteAt directly.
func TestConsistentWriteAtPastEnd(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source)
			name := filepath.Join(t.TempDir(), "file")
			if err := WriteFile(cfs, name, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := cfs.OpenFile(name, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			n, err := f.WriteAt([]byte("X"), 5)
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Errorf("write n = %d; want 1", n)
			}

			content, err := ReadFile(cfs, name)
			if err != nil {
				t.Fatal(err)
			}
			want := "abc\x00\x00X"
			if string(content) != want {
				t.Errorf("content = %q; want %q", content, want)
			}
		})
	}
}

// TestConsistentTruncateGrowth verifies that growing a file via Truncate
// preserves the original prefix and fills the grown region with zero bytes.
func TestConsistentTruncateGrowth(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source)
			name := filepath.Join(t.TempDir(), "file")
			if err := WriteFile(cfs, name, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := cfs.OpenFile(name, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if err := f.Truncate(6); err != nil {
				t.Fatal(err)
			}

			info, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != 6 {
				t.Errorf("size = %d; want 6", info.Size())
			}

			content, err := ReadFile(cfs, name)
			if err != nil {
				t.Fatal(err)
			}
			want := "abc\x00\x00\x00"
			if string(content) != want {
				t.Errorf("content = %q; want %q", content, want)
			}
		})
	}
}

// TestConsistentFillFailureReportsProgress verifies that when the second
// fill step fails, the adapter reports the actual fill progress (one byte)
// rather than claiming atomic success.
func TestConsistentFillFailureReportsProgress(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source).WithFillChunkSize(1)
			name := filepath.Join(t.TempDir(), "file")
			if err := WriteFile(cfs, name, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			// Open via the source directly so the failWriteFile wraps the
			// source file, not another ConsistentWriteFile (which would
			// double-fill).
			orig, err := source.OpenFile(name, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer orig.Close()

			// Wrap the file so the second fill step fails. The first fill
			// step is the first WriteAt call on the wrapped file; the second
			// fill step is the second WriteAt call.
			ff := &failWriteFile{File: orig, failAt: 2}
			wrapped := cfs.WrapFile(ff)

			if _, err := wrapped.Seek(5, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			n, err := wrapped.Write([]byte("X"))
			if err == nil {
				t.Fatal("expected injected fill failure, got nil")
			}
			if n != 1 {
				t.Errorf("progress n = %d; want 1 (one fill byte completed)", n)
			}

			// The file should contain the original "abc" plus one zero byte
			// from the first fill step; the second fill step failed.
			content, err := ReadFile(cfs, name)
			if err != nil {
				t.Fatal(err)
			}
			want := "abc\x00"
			if string(content) != want {
				t.Errorf("content = %q; want %q", content, want)
			}
		})
	}
}

// TestConsistentWriteShortWrite verifies that a short underlying write
// returns the actual number of bytes written.
func TestConsistentWriteShortWrite(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source).WithFillChunkSize(1)
			name := filepath.Join(t.TempDir(), "file")
			if err := WriteFile(cfs, name, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			// Open via the source directly so the failWriteFile wraps the
			// source file, not another ConsistentWriteFile (which would
			// double-fill).
			orig, err := source.OpenFile(name, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer orig.Close()

			// Wrap so the "X" write fails. The first two WriteAt calls are
			// the fill steps; the third is the "X" write. Failing it
			// simulates a short write.
			ff := &failWriteFile{File: orig, failAt: 3}
			wrapped := cfs.WrapFile(ff)

			if _, err := wrapped.Seek(5, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			n, err := wrapped.Write([]byte("X"))
			if err == nil {
				t.Fatal("expected injected write failure, got nil")
			}
			if n != 0 {
				t.Errorf("n = %d; want 0 (X was not written)", n)
			}
		})
	}
}

// TestConsistentWriteOverflow verifies that WriteAt rejects negative
// offsets and offset+length overflow before any computation.
func TestConsistentWriteOverflow(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source)
			name := filepath.Join(t.TempDir(), "file")
			f, err := cfs.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if _, err := f.WriteAt([]byte("x"), -1); !errors.Is(err, ErrOutOfRange) {
				t.Errorf("WriteAt(-1) err = %v; want ErrOutOfRange", err)
			}
			if _, err := f.WriteAt([]byte("x"), math.MaxInt64); !errors.Is(err, ErrTooLarge) {
				t.Errorf("WriteAt(MaxInt64) err = %v; want ErrTooLarge", err)
			}
		})
	}
}

// TestConsistentTruncateNegative verifies that Truncate rejects negative
// sizes.
func TestConsistentTruncateNegative(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source)
			name := filepath.Join(t.TempDir(), "file")
			f, err := cfs.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if err := f.Truncate(-1); !errors.Is(err, ErrOutOfRange) {
				t.Errorf("Truncate(-1) err = %v; want ErrOutOfRange", err)
			}
		})
	}
}

// TestConsistentCloseOwnership verifies that a borrowed file's Close does
// not close the underlying file, while an owned file's Close does.
func TestConsistentCloseOwnership(t *testing.T) {
	t.Run("owned", func(t *testing.T) {
		cfs := NewConsistentWriteFs(NewMemMapFs())
		f, err := cfs.Create("file")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		// The underlying file should be closed, so Read must return an
		// error (any error indicates the file is no longer usable).
		if _, err := f.Read(make([]byte, 1)); err == nil {
			t.Errorf("Read after Close err = nil; want an error")
		}
	})

	t.Run("borrowed", func(t *testing.T) {
		src := NewMemMapFs()
		orig, err := src.Create("file")
		if err != nil {
			t.Fatal(err)
		}
		cfs := NewConsistentWriteFs(src)
		wrapped := cfs.WrapFile(orig)
		if err := wrapped.Close(); err != nil {
			t.Fatal(err)
		}
		// The underlying file should still be open. Read on an empty file
		// returns io.EOF, which is not a "file is closed" error.
		if _, err := wrapped.Read(make([]byte, 1)); err != nil && !errors.Is(err, io.EOF) {
			t.Errorf("Read after borrowed Close err = %v; want nil or EOF", err)
		}
		// The original file should still be usable.
		if _, err := orig.Stat(); err != nil {
			t.Errorf("orig.Stat after borrowed Close err = %v; want nil", err)
		}
	})
}

// TestConsistentWriteString verifies that WriteString also zero-fills.
func TestConsistentWriteString(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source)
			name := filepath.Join(t.TempDir(), "file")
			if err := WriteFile(cfs, name, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := cfs.OpenFile(name, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if _, err := f.Seek(5, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteString("X"); err != nil {
				t.Fatal(err)
			}

			content, err := ReadFile(cfs, name)
			if err != nil {
				t.Fatal(err)
			}
			want := "abc\x00\x00X"
			if string(content) != want {
				t.Errorf("content = %q; want %q", content, want)
			}
		})
	}
}

// TestConsistentNoFillWhenNotPastEnd verifies that writes at or before the
// end of file do not introduce spurious zero bytes.
func TestConsistentNoFillWhenNotPastEnd(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source)
			name := filepath.Join(t.TempDir(), "file")
			if err := WriteFile(cfs, name, []byte("abcdef"), 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := cfs.OpenFile(name, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if _, err := f.WriteAt([]byte("XY"), 2); err != nil {
				t.Fatal(err)
			}

			content, err := ReadFile(cfs, name)
			if err != nil {
				t.Fatal(err)
			}
			want := "abXYef"
			if string(content) != want {
				t.Errorf("content = %q; want %q", content, want)
			}
		})
	}
}

// TestConsistentFillChunkSize verifies that WithFillChunkSize and
// FillChunkSize behave as expected.
func TestConsistentFillChunkSize(t *testing.T) {
	cfs := NewConsistentWriteFs(NewMemMapFs())
	if got := cfs.FillChunkSize(); got != defaultFillChunkSize {
		t.Errorf("default FillChunkSize = %d; want %d", got, defaultFillChunkSize)
	}
	cfs.WithFillChunkSize(1)
	if got := cfs.FillChunkSize(); got != 1 {
		t.Errorf("FillChunkSize = %d; want 1", got)
	}
	cfs.WithFillChunkSize(0) // ignored
	if got := cfs.FillChunkSize(); got != 1 {
		t.Errorf("FillChunkSize after 0 = %d; want 1", got)
	}
}

// TestConsistentWriteAtZeroLength verifies that zero-length writes do not
// grow the file.
func TestConsistentWriteAtZeroLength(t *testing.T) {
	for _, source := range []Fs{NewMemMapFs(), NewOsFs()} {
		t.Run(source.Name(), func(t *testing.T) {
			cfs := NewConsistentWriteFs(source)
			name := filepath.Join(t.TempDir(), "file")
			if err := WriteFile(cfs, name, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := cfs.OpenFile(name, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if _, err := f.WriteAt(nil, 100); err != nil {
				t.Fatal(err)
			}
			info, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != 3 {
				t.Errorf("size = %d; want 3", info.Size())
			}
		})
	}
}

// ExampleConsistentWriteFs demonstrates the six-byte example: after
// writing "abc", seeking to 5 and writing "X", the file reads back as
// "abc" plus two zero bytes plus "X".
func ExampleConsistentWriteFs() {
	cfs := NewConsistentWriteFs(NewMemMapFs())
	f, err := cfs.Create("example")
	if err != nil {
		panic(err)
	}
	if _, err := f.Write([]byte("abc")); err != nil {
		panic(err)
	}
	if _, err := f.Seek(5, io.SeekStart); err != nil {
		panic(err)
	}
	if _, err := f.Write([]byte("X")); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	content, err := ReadFile(cfs, "example")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q\n", content)
	// Output: "abc\x00\x00X"
}
