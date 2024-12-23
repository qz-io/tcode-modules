package pathutil

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type PathUtil struct {
	execPath    string
	workingPath string
	once        sync.Once
}

var instance *PathUtil
var once sync.Once

func GetInstance() *PathUtil {
	once.Do(func() {
		instance = &PathUtil{}
		instance.init()
	})
	return instance
}

func (pu *PathUtil) init() {
	pu.once.Do(func() {
		ex, err := os.Executable()
		if err == nil {
			pu.execPath = filepath.Dir(ex)
		} else {
			panic(err)
		}

		wd, err := os.Getwd()
		if err == nil {
			pu.workingPath = wd
		} else {
			panic(err)
		}

		err0 := os.MkdirAll(pu.workingPath+"/logs/tmp/", os.ModePerm)
		if err != nil {
			fmt.Printf("Failed to initialize temporary directory: %v\n", err0)
			panic("Failed to initialize temporary directory")
		}
	})
}

func (pu *PathUtil) GetExecPath() string {
	return pu.execPath
}

func (pu *PathUtil) GetWorkingPath() string {
	return pu.workingPath
}

func (pu *PathUtil) GetWorkingPathTemp() string {
	return pu.workingPath + "/logs/tmp"
}

func ParseDsFromPath(filePath string) string {
	dir := path.Dir(filePath)
	parts := strings.Split(dir, "/")
	if len(parts) > 1 {
		return parts[len(parts)-1]
	}
	return ""
}
