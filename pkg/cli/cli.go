package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ttn-nguyen42/go-mini-lsm/pkg/lsm"
)

func Run() {
	c := newCli()
	c.Loop()
}

type cli struct {
	lsm  lsm.LSM
	cmds []command
	buf  io.Writer
}

func newCli() *cli {
	c := &cli{
		lsm:  lsm.New(),
		cmds: make([]command, 0),
		buf:  os.Stdout,
	}
	c.register()
	return c
}

func (c *cli) register() {
	c.cmds = append(c.cmds, &helpCmd{buf: c.buf})
	c.cmds = append(c.cmds, &exitCmd{buf: c.buf})
	c.cmds = append(c.cmds, &putCmd{lsm: c.lsm, buf: c.buf})
	c.cmds = append(c.cmds, &getCmd{lsm: c.lsm, buf: c.buf})
	c.cmds = append(c.cmds, &delCmd{lsm: c.lsm, buf: c.buf})
}

func (c *cli) Loop() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Fprintln(c.buf, "Welcome to mini-lsm REPL!")
	fmt.Fprintln(c.buf, "Type 'help' for a list of commands.")
	fmt.Fprintf(c.buf, "Type 'exit' to quit.\n")

	for {
		fmt.Fprint(c.buf, "> ")

		if !scanner.Scan() {
			break
		}
		
		line := strings.TrimSpace(scanner.Text())
		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}

		matched := false
		for _, cmd := range c.cmds {
			if cmd.ShouldRun(args) {
				shouldContinue, err := cmd.Execute(args[1:])
				if err != nil {
					fmt.Fprintf(c.buf, "Error: %v\n", err)
				}
				matched = true
				if !shouldContinue {
					return
				}
				break
			}
		}
		if !matched {
			fmt.Fprintf(c.buf, "Unknown command: %s\n", args[0])
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(c.buf, "Error reading input: %v\n", err)
	}
}

type command interface {
	ShouldRun(args []string) bool
	Execute(args []string) (bool, error)
}
