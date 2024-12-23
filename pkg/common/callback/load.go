package callback

import (
	"fmt"
	"io/ioutil"
	"os"
	"plugin"
)

type p struct {
	tempDir string
}

func p0() *p {
	return &p{
		tempDir: os.TempDir(),
	}
}

func (p *p) l(s []byte) (*plugin.Plugin, error) {
	t, e := ioutil.TempFile(p.tempDir, "tm*")
	if e != nil {
		fmt.Printf("%v", e)
	}
	defer os.Remove(t.Name())

	if _, err := t.Write(s); err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	if err := t.Close(); err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	plug, e := plugin.Open(t.Name())
	if e != nil {
		return nil, fmt.Errorf("%v", e)
	}
	return plug, nil
}
