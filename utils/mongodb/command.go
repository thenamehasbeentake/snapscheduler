package mongodb

import (
	"time"

	"deeproute.ai/snapscheduler/utils/exec"
)

const (
	pbmCommand     = "pbm"
	defaultTimeout = 3 * time.Minute

	StatusDone      string = "done"
	StatusCancelled string = "canceled"
	StatusCopyReady string = "copyReady"
	StatusStarting  string = "starting"
	StatusRunning   string = "running"
	StatusCopyDone  string = "copyDone"
	StatusError     string = "error"
)

type PbmToolCommand struct {
	executor exec.Executor

	tool           string
	args           []string
	timeout        time.Duration
	JsonOutput     bool
	combinedOutput bool
}

func newPbmToolCommand(tool string, executor exec.Executor, args []string) *PbmToolCommand {
	return &PbmToolCommand{
		executor:       executor,
		tool:           tool,
		args:           args,
		JsonOutput:     true,
		combinedOutput: false,
		timeout:        defaultTimeout,
	}
}

func NewPbmCommand(executor exec.Executor, args []string) *PbmToolCommand {
	return newPbmToolCommand(pbmCommand, executor, args)
}

func (c *PbmToolCommand) run() ([]byte, error) {
	command, args := c.tool, c.args

	if c.JsonOutput {
		args = append(args, "--out", "json")
	}

	var output string
	var err error

	// NewRBDCommand does not use the --out-file option so we only check for remote execution here
	// Still forcing the check for the command if the behavior changes in the future
	if command == pbmCommand {
		if c.timeout == 0 {
			output, err = c.executor.ExecuteCommandWithOutput(command, args...)
		} else {
			output, err = c.executor.ExecuteCommandWithTimeout(c.timeout, command, args...)
		}
	} else if c.timeout == 0 {
		if c.combinedOutput {
			output, err = c.executor.ExecuteCommandWithCombinedOutput(command, args...)
		} else {
			output, err = c.executor.ExecuteCommandWithOutput(command, args...)
		}
	} else {
		output, err = c.executor.ExecuteCommandWithTimeout(c.timeout, command, args...)
	}

	return []byte(output), err
}
func (c *PbmToolCommand) RunWithTimeout(timeout time.Duration) ([]byte, error) {
	c.timeout = timeout
	return c.run()
}

type CommandOption func(*commandOptions)

type commandOptions struct {
	mongodbURI string
	timeout    time.Duration
}

func WithMongodbURI(mongodbURI string) CommandOption {
	return func(o *commandOptions) {
		o.mongodbURI = mongodbURI
	}
}

func buildArgs(baseArgs []string, opts ...CommandOption) []string {
	options := &commandOptions{
		timeout: defaultTimeout, // 默认值
	}

	for _, opt := range opts {
		opt(options)
	}

	args := make([]string, 0, len(baseArgs)+2) // 预分配空间

	// 添加公共参数
	if options.mongodbURI != "" {
		args = append(args, "--mongodb-uri", options.mongodbURI)
	}

	// 添加命令特定参数
	args = append(args, baseArgs...)

	return args
}
