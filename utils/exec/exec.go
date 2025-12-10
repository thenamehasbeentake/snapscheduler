package exec

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	kexec "k8s.io/utils/exec"
)

var (
	CephCommandsTimeout     = 15 * time.Second
	CephSlowCommandsTimeout = 300 * time.Second
	LvmCommandsTimeout      = 5 * time.Minute // TODO(zhengliang): make sure that deleting large lvs does not timeout
)

// Executor is the main interface for all the exec commands
type Executor interface {
	ExecuteCommand(command string, arg ...string) error
	ExecuteCommandWithEnv(env []string, command string, arg ...string) error
	ExecuteCommandWithOutput(command string, arg ...string) (string, error)
	ExecuteCommandWithCombinedOutput(command string, arg ...string) (string, error)
	ExecuteCommandWithTimeout(timeout time.Duration, command string, arg ...string) (string, error)
}

// CommandExecutor is the type of the Executor
type CommandExecutor struct{}

// ExecuteCommand starts a process and wait for its completion
func (c *CommandExecutor) ExecuteCommand(command string, arg ...string) error {
	return c.ExecuteCommandWithEnv([]string{}, command, arg...)
}

// ExecuteCommandWithEnv starts a process with env variables and wait for its completion
func (*CommandExecutor) ExecuteCommandWithEnv(env []string, command string, arg ...string) error {
	cmd, stdout, stderr, err := startCommand(env, command, arg...)
	if err != nil {
		return err
	}

	logOutput(stdout, stderr)

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

// ExecuteCommandWithTimeout starts a process and wait for its completion with timeout.
func (*CommandExecutor) ExecuteCommandWithTimeout(timeout time.Duration, command string, arg ...string) (string, error) {
	logCommand(command, arg...)
	cmd := exec.Command(command, arg...)

	var (
		stdoutBuf bytes.Buffer
		stderrBuf bytes.Buffer
	)
	cmd.Stdout = &stdoutBuf // https://stackoverflow.com/questions/77624066/why-is-the-exec-command-output-exceeding-65536-truncated-when-exec-cmd-stdout
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return "", err
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	interruptSent := false
	for {
		select {
		case <-time.After(timeout):
			if interruptSent {
				klog.Infof("timeout waiting for process %s to return after interrupt signal was sent. Sending kill signal to the process", command)
				var e error
				if err := cmd.Process.Kill(); err != nil {
					klog.Errorf("Failed to kill process %s: %v", command, err)
					e = fmt.Errorf("timeout waiting for the command %s to return after interrupt signal was sent. Tried to kill the process but that failed: %w", command, err)
				} else {
					e = fmt.Errorf("timeout waiting for the command %s to return", command)
				}
				return strings.TrimSpace(stdoutBuf.String()), e
			}

			klog.Infof("timeout waiting for process %s to return. Sending interrupt signal to the process", command)
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				klog.Errorf("Failed to send interrupt signal to process %s: %v", command, err)
				// kill signal will be sent next loop
			}
			interruptSent = true
		case err := <-done:
			if err != nil {
				return strings.TrimSpace(stderrBuf.String()), err
			}
			if interruptSent {
				return strings.TrimSpace(stdoutBuf.String()), fmt.Errorf("timeout waiting for the command %s to return", command)
			}
			return strings.TrimSpace(stdoutBuf.String()), nil
		}
	}
}

// ExecuteCommandWithOutput executes a command with output
func (*CommandExecutor) ExecuteCommandWithOutput(command string, arg ...string) (string, error) {
	logCommand(command, arg...)
	cmd := exec.Command(command, arg...)
	return runCommandWithOutput(cmd, false)
}

// ExecuteCommandWithCombinedOutput executes a command with combined output
func (*CommandExecutor) ExecuteCommandWithCombinedOutput(command string, arg ...string) (string, error) {
	logCommand(command, arg...)
	cmd := exec.Command(command, arg...)
	return runCommandWithOutput(cmd, true)
}

func startCommand(env []string, command string, arg ...string) (*exec.Cmd, io.ReadCloser, io.ReadCloser, error) {
	logCommand(command, arg...)

	cmd := exec.Command(command, arg...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		klog.Warningf("failed to open stdout pipe: %+v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		klog.Warningf("failed to open stderr pipe: %+v", err)
	}

	if len(env) > 0 {
		cmd.Env = env
	}

	err = cmd.Start()

	return cmd, stdout, stderr, err
}

// read from reader line by line and write it to the log
func logFromReader(reader io.ReadCloser) {
	in := bufio.NewScanner(reader)
	lastLine := ""
	for in.Scan() {
		lastLine = in.Text()
		klog.V(5).Infof(lastLine)
	}
}

func logOutput(stdout, stderr io.ReadCloser) {
	if stdout == nil || stderr == nil {
		klog.Warningf("failed to collect stdout and stderr")
		return
	}

	go logFromReader(stderr)
	logFromReader(stdout)
}

func runCommandWithOutput(cmd *exec.Cmd, combinedOutput bool) (string, error) {
	var output []byte
	var err error
	var out string

	if combinedOutput {
		output, err = cmd.CombinedOutput()
	} else {
		output, err = cmd.Output()
		if err != nil {
			output = []byte(fmt.Sprintf("%s. %s", string(output), assertErrorType(err)))
		}
	}

	out = strings.TrimSpace(string(output))

	if err != nil {
		return out, err
	}

	return out, nil
}

func logCommand(command string, arg ...string) {
	klog.Infof("Running command: %s %s", command, strings.Join(arg, " "))
}

func assertErrorType(err error) string {
	var exitErr *exec.ExitError
	var execErr *exec.Error

	if errors.As(err, &exitErr) {
		return strconv.Itoa(exitErr.ProcessState.ExitCode())
	}

	if errors.As(err, &execErr) {
		return execErr.Error()
	}

	return ""
}

// ExtractExitCode attempts to get the exit code from the error returned by an Executor function.
// This should also work for any errors returned by the golang os/exec package and "k8s.io/utils/exec"
func ExtractExitCode(err error) (int, error) {
	switch errType := err.(type) {
	case *exec.ExitError:
		return errType.ExitCode(), nil

	case *kexec.CodeExitError:
		return errType.ExitStatus(), nil

	// have to check both *kexec.CodeExitError and kexec.CodeExitError because CodeExitError methods
	// are not defined with pointer receivers; both pointer and non-pointers are valid `error`s.
	case kexec.CodeExitError:
		return errType.ExitStatus(), nil

	case *kerrors.StatusError:
		return int(errType.ErrStatus.Code), nil

	default:
		klog.V(5).Infof(err.Error())
		// This is ugly, but it's a decent backup just in case the error isn't a type above.
		if strings.Contains(err.Error(), "command terminated with exit code") {
			a := strings.SplitAfter(err.Error(), "command terminated with exit code")
			return strconv.Atoi(strings.TrimSpace(a[1]))
		}
		return -1, errors.Errorf("error %#v is an unknown error type: %v", err, reflect.TypeOf(err))
	}
}
