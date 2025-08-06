package model

import (
	"errors"
	"os/exec"
	"time"

	"github.com/qz-io/tcode-modules/pkg/util"
)

func GetVideoSize(v *VideoInfo) (int, int) {
	for _, s := range v.Streams {
		if s.CodecType == "video" && !util.IsCoverCodec(s.CodecName, s.BitRate) {
			if s.SideDataList != nil && len(s.SideDataList) > 0 {
				for _, side := range s.SideDataList {
					if side.Rotation == -90 {
						return s.Height, s.Width
					}
				}
			}
			return s.Width, s.Height
		}
	}

	return 0, 0
}

var FileNotFound = errors.New("FileNotFound")

type EnCoderTask struct {
	Cmd         *exec.Cmd
	FileUUid    string `json:"file_uuid"`
	TaskTid     string `json:"task_tid"`
	Definition  string `json:"definition"`
	PlayListUrl string
	Res         *ApiResponse
	OnGpuId     string
	ProgramId   string
	CreateTime  time.Time
	RunArgsIdx  uint64
}

type TranscodeModel int

const (
	Default TranscodeModel = iota
	Fast
	TCheck
)

type CodeRequest struct {
	Transcode        TranscodeModel
	Input            string     `json:"input"`
	MediaType        int        `json:"media_type"`
	FileUUid         string     `json:"file_uuid"`
	VideoType        string     `json:"video_type"`
	Definitions      []string   `json:"definitions"`
	ForceDefinition  string     `json:"force_definition"`
	TaskTid          string     `json:"task_tid"`
	IsUpload         bool       `json:"is_upload"`
	VideoInfo        *VideoInfo `json:"video_info"`
	Width            int
	Height           int
	DurationTs       int `json:"duration_ts"`
	CallBackPlayUrl  string
	CallBackIndexUrl string
	ApiReq           ApiRequest
	RunError         string
}

type ApiRequest struct {
	BeginTsIndex    int    `json:"begin_ts_index"`
	BeginTime       int    `json:"begin_time"`
	FirstTsSet      string `json:"first_ts_set"`
	TotalTime       int    `json:"total_size"`
	GOP             int    `json:"gop"`
	GT              int    `json:"gt"`
	UserId          string `json:"user_id"`
	FileUUid        string `json:"file_uuid"`
	Ci              string `json:"ci"`
	Url             string `json:"url"`
	Definition      string `json:"definition"`
	TaskTid         string `json:"task_tid"`
	TargetCodecType string `json:"target_codec_type"`
	TotalTsNumber   int
	SkipIds         []string
	PreTranscode    bool `json:"pre_transcode"`
}

type FastApiRequest struct {
	ApiRequest
}

type ApiProgressResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Times   int    `json:"times"`
}

type ApiTaskData struct {
	Uid        string `json:"uid"`
	Definition string `json:"definition"`
	TaskTid    string `json:"task_tid"`

	Status      int    `json:"status"`
	PlayListUrl string `json:"play_list_url"`
}

type ApiBaseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

type ApiActionResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Result  string `json:"result"`
}

type ApiResponse struct {
	Code    int      `json:"code"`
	Message string   `json:"message"`
	Data    *ApiData `json:"data"`
}

type ApiData struct {
	X           int    `json:"x"`
	Y           int    `json:"y"`
	Times       int    `json:"times"`
	Url         string `json:"url"`
	ProgressUid string `json:"progress_uid"`
	IndexUrl    string `json:"index_url"`
}

type Streams []struct {
	Index              int    `json:"index"`
	CodecName          string `json:"codec_name"`
	CodecType          string `json:"codec_type"`
	Profile            string `json:"profile"`
	PixFmt             string `json:"pix_fmt"`
	ColorSpace         string `json:"color_space"`
	RFrameRate         string `json:"r_frame_rate"`
	BitRate            string `json:"bit_rate"`
	Width              int
	Height             int
	DisplayAspectRatio string            `json:"display_aspect_ratio"`
	Tags               map[string]string `json:"tags"`
	Disposition        struct {
		Default int `json:"default"`
	} `json:"disposition"`
	SideDataList []SideData `json:"side_data_list"`
}

type SideData struct {
	Rotation int `json:"rotation"`
}
type VideoInfo struct {
	Streams `json:"streams"`
	Format  struct {
		MediaType          int    `json:"media_type"`
		NbStreams          int    `json:"nb_streams"`
		BitRate            string `json:"bit_rate"`
		Fps                int    `json:"fps"`
		Duration           string `json:"duration"`
		Size               string `json:"size"`
		TimeScale          int    `json:"time_scale"`
		AudioStreamNum     int    `json:"audio_stream_num"`
		AudioStreamPattern string `json:"audio_stream_pattern"`
		Tags               struct {
			Encoder           string `json:"encoder"`
			CreationTime      string `json:"creation_time"`
			Location          string `json:"location"`
			QuicktimeLocation string `json:"com.apple.quicktime.location.ISO6709"`
		} `json:"tags"`
	} `json:"format"`
}

type ResultType struct {
	Result string
	Err    error
	Index  int
}

type ReportStatusType string

const (
	Subtitle   ReportStatusType = "subtitle"
	Audio      ReportStatusType = "audio"
	Media      ReportStatusType = "media"
	PlayStatus ReportStatusType = "PlayStatus"
)

type StatusReportRequest struct {
	FileId     string           `json:"file_id"`
	StatusType ReportStatusType `json:"status_type"`
	Status     int              `json:"status"`
}
