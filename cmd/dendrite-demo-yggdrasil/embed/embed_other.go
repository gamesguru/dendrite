//go:build !elementweb
// +build !elementweb

package embed

import "codefloe.com/pat-s/dendrite/internal/httputil"

func Embed(_ *httputil.Router, _ int, _ string) {
}
