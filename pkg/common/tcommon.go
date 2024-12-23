package common

import (
	"context"

	"github.com/rs/zerolog"
)

var Logger *zerolog.Logger

var mDCLogger = "MDCLogger"

func WithMDC(ctx context.Context, key string, value string) (context.Context, *zerolog.Logger) {
	logger := GetLogger(ctx)
	log := Logger.With().Str(key, value).Timestamp().Caller().Logger()
	ctx = context.WithValue(ctx, mDCLogger, &log)
	return ctx, logger
}

func GetLogger(ctx context.Context) *zerolog.Logger {
	if ctx == nil {
		return Logger
	}

	mdcLog, ok := ctx.Value(mDCLogger).(*zerolog.Logger)
	if ok && mdcLog != nil {
		return mdcLog
	}

	return Logger
}

const (
	SkipPlayTaskDefaultFormat string = "skip_play_00000_"
)

var UseProxyCache = false
var UseHttpProxy = false
var DownloadHttpUrl = ""
var BaseHttpServer = ""
var BasePath = ""
var ReadPath = BasePath + "%s/default/%s/index.m3u8"
var AppMode = "production"
var TsTime = 5
var FirstTsSet = "2"
var UseGPU = false
var UseCuda = false
var UseGpuDecoder = true
var UseCpu = false
var MultiChannel = true
var AutoDownload = false
var DevUploadNumber = 0
var DevCoderTime = 0
var FastCoderTime = 10
var DevFstartRunModel = ""
var FfmpegPath = ""
var AacEncoder = ""
var CleanFastCache = false
var CloseMultiAudio = true
var CloseSubtitle = false
var ParallelAudioTranscode = false
var EnableFilter = true
