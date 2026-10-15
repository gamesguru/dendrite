// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package thumbnailer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

// MayAnimate reports whether a file of the content type can be an animated
// image, so that AnimatedImage is worth reading it for.
func MayAnimate(contentType string) bool {
	switch contentType {
	case "image/gif", "image/png", "image/apng", "image/webp":
		return true
	}
	return false
}

// AnimatedImage reports whether the image is an animated GIF, APNG or WebP
// (MSC2705) and, if so, its width and height. It reads only as far as it
// needs to. Other content, including images that cannot be parsed, is not
// animated.
func AnimatedImage(r io.Reader) (animated bool, width, height int) {
	reader := bufio.NewReader(r)
	header, _ := reader.Peek(30) //nolint:mnd
	switch {
	case len(header) >= 10 && (bytes.HasPrefix(header, []byte("GIF87a")) || bytes.HasPrefix(header, []byte("GIF89a"))):
		width = int(binary.LittleEndian.Uint16(header[6:8]))
		height = int(binary.LittleEndian.Uint16(header[8:10]))
		animated = gifHasFrames(reader, 2) //nolint:mnd
	case len(header) >= 24 && bytes.HasPrefix(header, []byte("\x89PNG\r\n\x1a\n")) && string(header[12:16]) == "IHDR":
		width = int(binary.BigEndian.Uint32(header[16:20]))
		height = int(binary.BigEndian.Uint32(header[20:24]))
		animated = pngIsAnimated(reader)
	case len(header) >= 30 && string(header[:4]) == "RIFF" && string(header[8:12]) == "WEBP" && string(header[12:16]) == "VP8X":
		// The animation flag is bit 1 of the VP8X chunk, which must come
		// first, followed by the 24-bit canvas width and height minus one.
		animated = header[20]&(1<<1) != 0
		width = int(uint32(header[24])|uint32(header[25])<<8|uint32(header[26])<<16) + 1
		height = int(uint32(header[27])|uint32(header[28])<<8|uint32(header[29])<<16) + 1
	}
	if !animated {
		return false, 0, 0
	}
	return true, width, height
}

// GIF block introducers.
const (
	gifImageDescriptor = 0x2C
	gifExtension       = 0x21
)

// gifHasFrames reports whether the GIF holds at least n images, reading block
// headers only.
func gifHasFrames(r *bufio.Reader, n int) bool {
	var screen [13]byte // signature and logical screen descriptor
	if _, err := io.ReadFull(r, screen[:]); err != nil {
		return false
	}
	if skipColorTable(r, screen[10]) != nil {
		return false
	}
	frames := 0
	for {
		introducer, err := r.ReadByte()
		if err != nil {
			return false
		}
		switch introducer {
		case gifImageDescriptor:
			frames++
			if frames >= n {
				return true
			}
			var desc [9]byte
			if _, err = io.ReadFull(r, desc[:]); err != nil {
				return false
			}
			if skipColorTable(r, desc[8]) != nil {
				return false
			}
			if _, err = r.ReadByte(); err != nil { // LZW minimum code size
				return false
			}
			if skipSubBlocks(r) != nil {
				return false
			}
		case gifExtension:
			if _, err = r.ReadByte(); err != nil { // label
				return false
			}
			if skipSubBlocks(r) != nil {
				return false
			}
		default: // trailer or garbage
			return false
		}
	}
}

func skipColorTable(r *bufio.Reader, flags byte) error {
	if flags&0x80 == 0 {
		return nil
	}
	size := 3 * (1 << (int(flags&0x07) + 1)) //nolint:mnd
	_, err := r.Discard(size)
	return err
}

func skipSubBlocks(r *bufio.Reader) error {
	for {
		size, err := r.ReadByte()
		if err != nil {
			return err
		}
		if size == 0 {
			return nil
		}
		if _, err = r.Discard(int(size)); err != nil {
			return err
		}
	}
}

// pngIsAnimated reports whether an acTL chunk with more than one frame comes
// before the image data, which makes the PNG an APNG.
func pngIsAnimated(r *bufio.Reader) bool {
	if _, err := r.Discard(8); err != nil { //nolint:mnd
		return false
	}
	var chunk [8]byte
	for {
		if _, err := io.ReadFull(r, chunk[:]); err != nil {
			return false
		}
		length := binary.BigEndian.Uint32(chunk[:4])
		switch string(chunk[4:]) {
		case "acTL":
			var frames [4]byte
			if _, err := io.ReadFull(r, frames[:]); err != nil {
				return false
			}
			return binary.BigEndian.Uint32(frames[:]) > 1
		case "IDAT", "IEND":
			return false
		}
		// Skip the data and the CRC.
		if _, err := r.Discard(int(length) + 4); err != nil { //nolint:mnd
			return false
		}
	}
}
