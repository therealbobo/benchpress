package cmdinfo

import (
	"bytes"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

type CmdInfo struct {
	mu          sync.Mutex
	Name        string    `yaml:"name"`
	CmdStr      string    `yaml:"cmd"`
	Args        []string  `yaml:"args"`
	WorkDir     string    `yaml:"workdir"`
	Env         []string  `yaml:"env"`
	Cmd         *exec.Cmd `yaml:"-"`
	Stdout      string    `yaml:"-"`
	Stderr      string    `yaml:"-"`
	ElapsedTime float64   `yaml:"-"`
	KernelTime  float64   `yaml:"-"`
	UserTime    float64   `yaml:"-"`
}

func timevalToSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}

func waitAndGetRUsage(pid int) (float64, float64, error) {
	var rusage syscall.Rusage
	_, err := syscall.Wait4(pid, nil, 0, &rusage)
	if err != nil {
		return 0, 0, err
	}
	return timevalToSeconds(rusage.Stime), timevalToSeconds(rusage.Utime), nil
}

func (c *CmdInfo) Exec(wg *sync.WaitGroup, waitChan chan error) {
	defer wg.Done()

	// if there is no cmd, just exit
	if c.CmdStr == "" {
		waitChan <- nil
		log.Info().Str("started no-op", c.Name).Msg("")
		return
	}

	cmd := exec.Command(c.CmdStr, c.Args...)
	if c.WorkDir != "" {
		cmd.Dir = c.WorkDir
	}
	if len(c.Env) != 0 {
		cmd.Env = c.Env
	}


	c.mu.Lock()
	c.Cmd = cmd
	c.mu.Unlock()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	startTime := time.Now()
	err := cmd.Start()
	if err != nil {
		waitChan <- err
		return
	}
	waitChan <- nil

	log.Info().Str("started", c.Name).Msg("")

	kernelTime, userTime, err := waitAndGetRUsage(cmd.Process.Pid)
	if err != nil {
		waitChan <- err
		return
	}

	cmd.Wait()
	elapsed := time.Since(startTime)

	c.KernelTime = kernelTime
	c.UserTime = userTime
	c.ElapsedTime = elapsed.Seconds()

	log.Info().Str("finished", c.Name).Float64("kernel time", c.KernelTime).
		Float64("user time", c.UserTime).Float64("elapsed time", c.ElapsedTime).Msg("")

	c.Stdout = stdout.String()
	c.Stderr = stderr.String()
}

func (c *CmdInfo) Signal(sig syscall.Signal) {
	if c.Cmd != nil {
		c.mu.Lock()
		// TODO: make it configurable
		c.Cmd.Process.Signal(sig)
		c.mu.Unlock()
	}
}
