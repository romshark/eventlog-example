package cli

import (
	"bufio"
	"errors"
	"os"
	"strings"
)

// ScanLines calls onInput for every line scanned from os.Stdin.
func ScanLines(onInput func(line string) error) (err error) {
	reader := bufio.NewReader(os.Stdin)
	var ln string
	for {
		ln, err = reader.ReadString('\n')
		if err != nil {
			return err
		}
		ln = strings.Replace(ln, "\n", "", -1)
		if err = onInput(ln); err != nil {
			break
		}
	}
	if err == ErrAbortScan {
		err = nil
	}
	return
}

var ErrAbortScan = errors.New("abort scan")
