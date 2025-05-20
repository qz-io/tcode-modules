package common

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
)

func Mkdir(path string) {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		fmt.Printf("Mkdir error:%v", err)
	}
}

func DownloadFile(url string, filepath string) (int64, error) {
	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		Logger.Error().Msgf("Create file error:%v", err)
		return 0, err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	size, err1 := io.Copy(out, resp.Body)

	if err1 != nil {
		return 0, err1
	}

	return size, nil
}

func ExecCmd(cmd string) (string, int) {
	command := exec.Command("/bin/bash", "-c", cmd)
	out, err := command.CombinedOutput()
	if err != nil {
		if Logger != nil {
			Logger.Info().Msgf("ExecCmd error:%v", err)
		}

		return string(out), err.(*exec.ExitError).ExitCode()
	}
	return string(out), command.ProcessState.ExitCode()
}

var R0 func(o, t, f string) (string, error)

func ExecFfProbe(file string) (string, int) {
	command := exec.Command("ffprobe", "-hide_banner", "-i", file)
	out, err := command.CombinedOutput()
	if err != nil {
		if Logger != nil {
			Logger.Info().Msgf("ExecFfProbe error:%v", err)
		}
		return string(out), err.(*exec.ExitError).ExitCode()
	}
	return string(out), command.ProcessState.ExitCode()
}
