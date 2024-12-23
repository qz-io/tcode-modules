package callback

import (
	"context"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qz-io/tcode-modules/pkg/model"
)

type TsDesc struct {
	Name             string
	FilePath         string
	ObsPath          string
	SignUrlFullPath  string
	SignUrlShortPath string
}

type BaseProgressStatusService interface {
	LoadEndList() bool
	StoreEndList(val bool)
}

type BaseProgressStatus struct {
	endList           atomic.Bool
	CacheTs           chan string
	BuildFileBasePath string
	IndexPATH         string
	FileList          *model.SafeList
	TsUrlMap          *sync.Map
}

func (bs *BaseProgressStatus) LoadEndList() bool {
	return bs.endList.Load()
}

func (bs *BaseProgressStatus) StoreEndList(val bool) {
	bs.endList.Store(val)
}

type ProgressStatus struct {
	BaseProgressStatus
	FirstDoneFlag     bool
	CurrentDefinition string
}

type AudioSubTitleProgressStatus struct {
	BaseProgressStatus
	TypeName     string
	CurrentIndex string
}

type DefaultLogWriter struct {
	FileUid string
}

func (d *DefaultLogWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

type TsOperator interface {
	TsOperator(filename string, isFirst bool, tid string)
}

type ProgressWriter struct {
	TsOperatorImpl      TsOperator
	MultiChannel        bool
	Wg                  *sync.WaitGroup
	onceVD              sync.Once
	once                sync.Once
	Flag                atomic.Bool
	SingleFileList      *model.SafeList
	SingleTsUrlMap      *sync.Map
	TaskListFlag        bool
	CacheTs             chan string
	CurrentDefinition   string
	ProgressStatus      []*ProgressStatus
	AudioSubTilePsOpt   []*AudioSubTitleProgressStatus
	DefinitionsStatsRw  sync.RWMutex
	FirstPlayFileUrl    string
	FileUid             string
	Times               int
	IndexUrl            string
	SingleChanelENDLIST atomic.Bool
	IndexPATH           string
	TaskTid             string
	CodeRequest         *model.CodeRequest
	exitFlag            atomic.Bool
	CurrnetCmd          *exec.Cmd
	LockPlayStatus      bool
	StartTime           time.Time
	InnerData           *sync.Map
	Ctx                 context.Context
}

func (p *ProgressWriter) SetVDFirstDone() {
	p.onceVD.Do(func() {
		p.Wg.Done()
	})
}

func (p *ProgressWriter) SetFirstDone() {
	p.once.Do(func() {
		p.Wg.Done()
	})
}

func (p *ProgressWriter) SetExitFlag() {
	p.exitFlag.Store(true)
}

func (p *ProgressWriter) ExitFlag() bool {
	return p.exitFlag.Load()
}
