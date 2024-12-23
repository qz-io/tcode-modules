package validate

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func CheckSyspar() (int, error) {
	cmd := exec.Command("sysctl", "net.core.somaxconn")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get net.core.somaxconn: %v", err)
	}

	parts := strings.Split(string(output), "=")
	if len(parts) != 2 {
		return 0, fmt.Errorf("failed to parse net.core.somaxconn output: %s", output)
	}

	value, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, fmt.Errorf("failed to parse net.core.somaxconn value: %v", err)
	}

	return value, nil
}

func CheckUlimit() (int, error) {
	cmd := exec.Command("bash", "-c", "ulimit -n")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get ulimit -n: %v", err)
	}

	value, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		return 0, fmt.Errorf("failed to parse ulimit -n value: %v", err)
	}
	return value, nil
}
