package common

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
)

func cleanUpTempfile(f *os.File) {
	f.Close()
	os.Remove(f.Name())
}

type SubprocFate int

const (
	SubprocFateSucceeded SubprocFate = iota
	SubprocFateFailed    SubprocFate = iota
	SubprocFateCancelled SubprocFate = iota
	SubprocFateNoop      SubprocFate = iota
)

func (self SubprocFate) String() string {
	switch self {
	case SubprocFateSucceeded:
		return "succeeded"
	case SubprocFateFailed:
		return "failed"
	case SubprocFateCancelled:
		return "cancelled"
	case SubprocFateNoop:
		return "noop"
	default:
		panic("Unhandled SubprocFate value")
	}
}

// Jobber convention: a subprocess exit code of 200 means "no-op" (neither success nor error).
// This is useful for idempotent jobs that frequently have nothing to do and should not trigger
// notifyOnSuccess/notifyOnError.
const JobberExitNoop = 200

// An ExecResult describes the result of running a subprocess via ExecAndWait.
// Stdout and Stderr can be used to get the subprocess's stdout and stderr.
// When done with this object (e.g., when done reading Stdout/Stderr), you must
// call Close.
type ExecResult struct {
	Stdout io.ReadSeeker
	Stderr io.ReadSeeker
	Fate   SubprocFate
}

// MakeRunID generates a unique, opaque run identifier suitable for passing to job subprocesses.
func MakeRunID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (self *ExecResult) Close() {
	f := func(field *io.ReadSeeker) {
		if *field == nil {
			return
		}
		file := (*field).(*os.File)
		cleanUpTempfile(file)
		*field = nil
	}
	f(&self.Stdout)
	f(&self.Stderr)
}

func (self *ExecResult) _read(max int, f io.ReadSeeker) ([]byte, error) {
	buf := make([]byte, max)
	len, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:len], nil
}

func (self *ExecResult) ReadStdout(max int) (data []byte, err error) {
	return self._read(max, self.Stdout)
}

func (self *ExecResult) ReadStderr(max int) ([]byte, error) {
	return self._read(max, self.Stderr)
}

func MakeCmdExitedChan(cmd *exec.Cmd) <-chan error {
	c := make(chan error, 1)
	go func() {
		c <- cmd.Wait()
		close(c)
	}()
	return c
}

/*
Returns an unstarted process descriptor.
*/
func Sudo(usr user.User, cmdStr string) *exec.Cmd {
	return su_cmd(usr.Username, cmdStr, "/bin/sh")
}

func ExecAndWaitContextWithEnv(ctx context.Context, args []string, input []byte, env []string) (*ExecResult, error) {
	var cmd *exec.Cmd
	var newCtx context.Context
	var cancelSubproc context.CancelFunc
	if ctx == nil {
		cmd = exec.Command(args[0], args[1:]...)
	} else {
		newCtx, cancelSubproc = context.WithCancel(context.Background())
		defer cancelSubproc()
		cmd = exec.CommandContext(newCtx, args[0], args[1:]...)
	}
	if env != nil {
		cmd.Env = env
	}

	// make temp files for stdout/stderr
	stdout, err := ioutil.TempFile(TempDirPath(), "")
	if err != nil {
		return nil, err
	}
	stderr, err := ioutil.TempFile(TempDirPath(), "")
	if err != nil {
		cleanUpTempfile(stdout)
		return nil, err
	}

	// give them to cmd
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	// make stdin pipe
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("Failed to get pipe to stdin: %v", err)
	}

	// start cmd
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("Failed to fork: %v", err)
	}

	// write input
	stdin.Write(input)
	stdin.Close()

	// launch cancelling thread
	didCancel := false
	if ctx != nil {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				cancelSubproc()
				didCancel = true
				return
			case <-stop:
				return
			}
		}()
	}

	// finish execution
	waitErr := cmd.Wait()
	if waitErr != nil {
		ErrLogger.Printf("ExecAndWait: %v: %v", cmd.Path, waitErr)
		if _, ok := waitErr.(*exec.ExitError); !ok {
			return nil, fmt.Errorf("Failed to fork: %v", waitErr)
		}
	}

	// seek in stdout/stderr
	if _, err := stdout.Seek(0, 0); err != nil {
		cleanUpTempfile(stdout)
		cleanUpTempfile(stderr)
		return nil, fmt.Errorf("Failed to seek stdout: %v", err)
	}
	if _, err := stderr.Seek(0, 0); err != nil {
		cleanUpTempfile(stdout)
		cleanUpTempfile(stderr)
		return nil, fmt.Errorf("Failed to seek stderr: %v", err)
	}

	// return result
	res := &ExecResult{}
	res.Stdout = stdout
	res.Stderr = stderr
	if waitErr == nil {
		res.Fate = SubprocFateSucceeded
	} else if didCancel {
		res.Fate = SubprocFateCancelled
	} else {
		// Distinguish "no-op" from "failed" based on a reserved exit code.
		// Only applies to ExitError; other errors are handled above.
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			if exitErr.ProcessState != nil && exitErr.ProcessState.ExitCode() == JobberExitNoop {
				res.Fate = SubprocFateNoop
			} else {
				res.Fate = SubprocFateFailed
			}
		} else {
			res.Fate = SubprocFateFailed
		}
	}
	return res, nil
}

func ExecAndWaitContext(ctx context.Context, args []string, input []byte) (*ExecResult, error) {
	return ExecAndWaitContextWithEnv(ctx, args, input, nil)
}

func ExecAndWait(args []string, input []byte) (*ExecResult, error) {
	return ExecAndWaitContext(nil, args, input)
}
