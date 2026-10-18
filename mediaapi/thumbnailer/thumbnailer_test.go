package thumbnailer

import (
	"bytes"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadWebP(t *testing.T) {
	for _, animated := range []bool{false, true} {
		name, size, red := "still", 2, image.Pt(0, 0)
		if animated {
			name, size, red = "animated", 4, image.Pt(2, 2)
		}
		t.Run(name, func(t *testing.T) {
			img, err := readFile("testdata/" + name + ".webp")
			require.NoError(t, err)
			require.Equal(t, image.Rect(0, 0, size, size), img.Bounds())
			require.Equal(t, color.NRGBA{R: 255, A: 255}, color.NRGBAModel.Convert(img.At(red.X, red.Y)))
			if animated {
				// The first frame occupies the lower-right quadrant. Later frames are blue.
				_, _, _, alpha := img.At(0, 0).RGBA()
				require.Zero(t, alpha)
			}
			// Exercise the JPEG thumbnail path, not just decoding.
			path := filepath.Join(t.TempDir(), "thumbnail.jpg")
			require.NoError(t, writeFile(img, path))
			thumbnail, err := readFile(path)
			require.NoError(t, err)
			require.Equal(t, img.Bounds(), thumbnail.Bounds())
		})
	}
}

func TestReadOtherImageFormats(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	for _, format := range []string{"png", "jpeg", "gif"} {
		t.Run(format, func(t *testing.T) {
			var data bytes.Buffer
			var err error
			switch format {
			case "png":
				err = png.Encode(&data, img)
			case "jpeg":
				err = jpeg.Encode(&data, img, nil)
			case "gif":
				err = gif.Encode(&data, img, nil)
			}
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "image")
			require.NoError(t, os.WriteFile(path, data.Bytes(), 0o600))
			decoded, err := readFile(path)
			require.NoError(t, err)
			require.Equal(t, img.Bounds(), decoded.Bounds())
		})
	}
}

func TestReadInvalidWebP(t *testing.T) {
	data, err := os.ReadFile("testdata/animated.webp")
	require.NoError(t, err)
	for _, data := range [][]byte{nil, []byte("RIFF"), []byte("RIFF0000WEBPbroken"), data[:40]} {
		path := filepath.Join(t.TempDir(), "invalid.webp")
		require.NoError(t, os.WriteFile(path, data, 0o600))
		_, err := readFile(path)
		require.Error(t, err, "input length %d", len(data))
	}
}
