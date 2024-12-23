package util

import "strings"

var coverCodecs = map[string]struct{}{
	"mjpeg": {},
	"png":   {},
	"jpeg":  {},
	"jpg":   {},
	"gif":   {},
	"bmp":   {},
	"tiff":  {},
	"webp":  {},
	"apic":  {},
}

func IsCoverCodec(codeName, bitRate string) bool {
	if strings.ToLower(codeName) == "mjpeg" {
		if bitRate == "" || bitRate == "0" {
			return true
		} else {
			return false
		}
	}
	_, exists := coverCodecs[strings.ToLower(codeName)]
	return exists
}

func GetAllCoverCodecs() []string {
	codecs := make([]string, 0, len(coverCodecs))
	for codec := range coverCodecs {
		codecs = append(codecs, codec)
	}
	return codecs
}
