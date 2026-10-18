// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package thumbnailer

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/color"
	"image/color/palette"
	"image/gif"
	"image/png"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testGIF returns a GIF with the given number of frames.
func testGIF(t *testing.T, frames int) []byte {
	t.Helper()
	anim := &gif.GIF{}
	for i := range frames {
		img := image.NewPaletted(image.Rect(0, 0, 8, 8), palette.Plan9)
		img.Set(i%8, i%8, color.White)
		anim.Image = append(anim.Image, img)
		anim.Delay = append(anim.Delay, 10)
	}
	var buf bytes.Buffer
	require.NoError(t, gif.EncodeAll(&buf, anim))
	return buf.Bytes()
}

// apng returns a PNG with an acTL chunk announcing the given frame count.
func apng(t *testing.T, frames uint32) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 4, 4))))
	raw := buf.Bytes()
	// The IHDR chunk is 8 (signature) + 25 bytes long; insert acTL after it.
	insertAt := 8 + 25
	var acTL bytes.Buffer
	_ = binary.Write(&acTL, binary.BigEndian, uint32(8))
	acTL.WriteString("acTL")
	_ = binary.Write(&acTL, binary.BigEndian, frames)
	_ = binary.Write(&acTL, binary.BigEndian, uint32(0))
	_ = binary.Write(&acTL, binary.BigEndian, uint32(0)) // CRC, not checked
	return append(append(append([]byte{}, raw[:insertAt]...), acTL.Bytes()...), raw[insertAt:]...)
}

func TestIsAnimated(t *testing.T) {
	t.Parallel()
	var stillPNG bytes.Buffer
	require.NoError(t, png.Encode(&stillPNG, image.NewRGBA(image.Rect(0, 0, 4, 4))))
	animatedWebP, err := os.ReadFile("testdata/animated.webp")
	require.NoError(t, err)
	stillWebP, err := os.ReadFile("testdata/still.webp")
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		data []byte
		want bool
		w, h int
	}{
		{name: "animated gif", data: testGIF(t, 3), want: true, w: 8, h: 8},
		{name: "single frame gif", data: testGIF(t, 1)},
		{name: "apng", data: apng(t, 2), want: true, w: 4, h: 4},
		{name: "single frame apng", data: apng(t, 1)},
		{name: "png", data: stillPNG.Bytes()},
		{name: "animated webp", data: animatedWebP, want: true, w: 4, h: 4},
		{name: "still webp", data: stillWebP},
		{name: "truncated gif", data: testGIF(t, 3)[:20]},
		{name: "text", data: []byte("hello")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, w, h := AnimatedImage(bytes.NewReader(tc.data))
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.w, w)
			assert.Equal(t, tc.h, h)
		})
	}
}
