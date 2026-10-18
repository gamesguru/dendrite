# WebP fixtures

These synthetic fixtures contain solid colors and were generated with libwebp tools.
`still.webp` is a lossless 2×2 red image encoded with `cwebp -lossless`.
`animated.webp` has a transparent 4×4 canvas, a 2×2 red first frame at offset (2, 2), and a 4×4 blue second frame.
Each frame lasts 100 ms, with no blending or disposal.
This checks that thumbnail decoding returns the first composited frame, including its offset and untouched canvas.

```sh
webpmux -frame still.webp +100+2+2+0-b -frame blue.webp +100+0+0+0-b -bgcolor 0,0,0,0 -loop 0 -o animated.webp
```
