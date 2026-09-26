package afero_test

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/afero"
)

// demoCopy copies src to dst through the given Fs using a fixed-size buffer.
func demoCopy(fs afero.Fs, dstName, srcName string, bufSize int) error {
	src, err := fs.Open(srcName)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := fs.Create(dstName)
	if err != nil {
		return err
	}
	defer dst.Close()

	buf := make([]byte, bufSize)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			if ew != nil {
				return ew
			}
			if nw != nr {
				return io.ErrShortWrite
			}
		}
		if errors.Is(er, io.EOF) {
			return nil
		}
		if er != nil {
			return er
		}
	}
}

func demoRead(fs afero.Fs, name string) string {
	f, err := fs.Open(name)
	if err != nil {
		return "<missing>"
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return "<unreadable>"
	}
	return string(b)
}

func ExampleFaultInjectingFs() {
	errNoSpace := errors.New("no space left on device")

	// 1) A normal copy succeeds end to end.
	clean := afero.NewMemMapFs()
	_ = afero.WriteFile(clean, "src", []byte("abcdefghij"), 0o644)
	if err := demoCopy(afero.NewFaultInjectingFs(clean), "dst", "src", 5); err != nil {
		fmt.Println("normal copy error:", err)
	} else {
		fmt.Println("normal copy result:", demoRead(clean, "dst"))
	}

	// 2) Fault injection: on the second Write to "dst", write only the first
	// 3 bytes of the payload and then fail with a disk-full error.
	buggy := afero.NewMemMapFs()
	_ = afero.WriteFile(buggy, "src", []byte("abcdefghij"), 0o644)
	fs := afero.NewFaultInjectingFs(buggy, afero.FaultRule{
		Op:          afero.InjectWrite,
		Path:        "dst",
		N:           2,
		WritePrefix: 3,
		Err:         errNoSpace,
	})
	err := demoCopy(fs, "dst", "src", 5)
	fmt.Println("faulty copy error:", err)
	fmt.Println("faulty copy result:", demoRead(buggy, "dst"))

	// Output:
	// normal copy result: abcdefghij
	// faulty copy error: no space left on device
	// faulty copy result: abcdefgh
}
