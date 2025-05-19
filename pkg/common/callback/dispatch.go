package callback

import (
	"fmt"
	"plugin"
	"runtime/debug"
	"time"

	"embed"

	"github.com/qz-io/tcode-modules/pkg/common"
)

//go:embed all:zso/d
var pluginFS embed.FS

//go:embed all:zso/r
var pluginFSRelease embed.FS

//go:embed all:zso/f
var pluginFSFunc embed.FS

func d1() ([]byte, error) {
	return pluginFS.ReadFile("zso/d")
}

func r1() ([]byte, error) {
	return pluginFSRelease.ReadFile("zso/r")
}

func d() []byte {
	bs, err := d1()
	if err != nil {
		fmt.Printf("d %v\n", err)
		return nil
	}
	return bs
}

func r() []byte {
	bs, err := r1()
	if err != nil {
		fmt.Printf("r %v\n", err)
		return nil
	}
	return bs
}

func fc() []byte {
	bs, err := pluginFSFunc.ReadFile("zso/f")
	if err != nil {
		fmt.Printf("f %v\n", err)
		return nil
	}
	return bs
}

func l() func([]byte) (*plugin.Plugin, error) {
	l := p0()
	p0, err := l.l(d())
	if err != nil {
		p0, err = l.l(r())
		if err != nil {
			fmt.Printf("Failed to load p0: %v\n", err)
			return nil
		}
	}
	fmt.Println("Load p0so successfully")

	s, err := p0.Lookup("LoadPlugin0")
	if err != nil {
		fmt.Printf("%v\n", err)
		return nil
	}
	return s.(func([]byte) (*plugin.Plugin, error))
}

var (
	writeOptFunc func(*ProgressWriter, []byte)
)

func LoadPlugin(bs []byte) (*plugin.Plugin, func([]byte) (*plugin.Plugin, error)) {

	l0 := l()

	p, err := l0(bs)
	if err != nil {
		fmt.Printf("lp版本错误 %v\n", err)
		return nil, nil
	}

	w, err := p.Lookup("WriteOpt")
	if err != nil {
		fmt.Printf("%v\n", err)
		return nil, nil
	}

	writeOptFunc = *w.(*func(*ProgressWriter, []byte))
	fmt.Println("loaded successfully")

	return p, l0
}

func LoadFuncPlugin() (*plugin.Plugin, func([]byte) (*plugin.Plugin, error)) {

	l0 := l()

	p, err := l0(fc())
	if err != nil {
		fmt.Printf("lpversion %v\n", err)
		return nil, nil
	}

	w, err := p.Lookup("WriteOpt")
	if err != nil {
		fmt.Printf("%v\n", err)
		return nil, nil
	}

	writeOptFunc = *w.(*func(*ProgressWriter, []byte))
	fmt.Println("loaded successfully")

	return p, l0
}

func (p *ProgressWriter) SetImpl(impl TsOperator) {
	p.TsOperatorImpl = impl
}

func (p *ProgressWriter) Write(b []byte) (int, error) {
	defer func() {
		if r := recover(); r != nil {

			if err, ok := r.(error); ok {
				common.Logger.Err(err).Msgf("Recovered from Write")
			} else {
				common.Logger.Error().Msgf("Recovered from Write:%v", r)
			}

			debug.PrintStack()
		}

	}()

	if writeOptFunc != nil {
		p.StartTime = time.Now()
		writeOptFunc(p, b)
	} else {
		common.Logger.Error().Msg("not initialized")
		return 0, fmt.Errorf("plugin function not initialized")
	}

	return len(b), nil
}
