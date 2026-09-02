package utils

import (
	"math/rand"
	"regexp"
	"strings"
	"time"
)

func RandStringBytes(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// RandStringByNumLowercase 生成指定长度的随机字符串，包含小写字母和数字
func RandStringByNumLowercase(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz0123456789"
	// 创建一个本地的随机数生成器
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[r.Intn(len(letterBytes))]
	}
	return string(b)
}

var rfc1123Pattern = regexp.MustCompile(`[^a-z0-9-]+`)

func ToRFC1123Name(s string) string {
	s = strings.ToLower(s)
	s = rfc1123Pattern.ReplaceAllString(s, "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	s = strings.Trim(s, "-")
	if len(s) == 0 {
		return "port"
	}
	return s
}
