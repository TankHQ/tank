import { valueNoise } from './util.js'

// A layered, procedurally generated mountain backdrop.
//
// The mountains are not sprites: each layer is a 1-D height field sampled from
// seeded value noise, so the range is infinite and never repeats. Two tricks
// turn plain noise into mountains:
//
//   * fractal Brownian motion (fBm) - sum several octaves of noise (big slow
//     waves plus finer detail) so a ridge has both its overall mass and its
//     rocky texture;
//   * ridging - feed `1 - |2n - 1|` instead of `n`, which folds the smooth
//     bumps into sharp creases, i.e. the silhouette of a peak line.
//
// Layers are drawn back-to-front, each with its own frequency, amplitude,
// scroll speed (parallax) and haze tint, so distant ranges sit high, pale and
// slow while near ones are dark, tall and fast. Height is only ever a function
// of world x, so the backdrop scrolls seamlessly with the camera.
export class Background {
  constructor(cfg) {
    this.cfg = cfg
    this._buf = null
    this._bufCtx = null
  }

  // Ridged fBm in [0, 1], 1 for a sharp peak, 0 for a valley floor.
  _ridge(x, layer) {
    const { octaves, persistence, lacunarity } = this.cfg
    let sum = 0
    let norm = 0
    let amp = 1
    let freq = layer.frequency
    for (let o = 0; o < octaves; o++) {
      // Offset each octave and each layer into a different noise region.
      const n = valueNoise(x * freq + layer.seed + o * 37.13)
      const ridged = 1 - Math.abs(n * 2 - 1)
      sum += ridged * amp
      norm += amp
      amp *= persistence
      freq *= lacunarity
    }
    return sum / norm
  }

  // Screen-space height line for one layer, as {x, y} points across the canvas.
  _layerPath(W, H, cameraX, layer) {
    const horizonY = H * this.cfg.horizon
    const step = this.cfg.step
    const pts = []
    for (let sx = -step; sx <= W + step; sx += step) {
      const wx = cameraX * layer.parallax + sx
      const t = this._ridge(wx, layer)
      // Ease the ridge so the base is broad and the peaks are pointed.
      const y = horizonY - (layer.amplitude * H) * Math.pow(t, this.cfg.sharpness)
      pts.push({ x: sx, y })
    }
    return { pts, horizonY }
  }

  _paint(ctx, W, H, cameraX) {
    const { layers } = this.cfg
    for (const layer of layers) {
      const { pts, horizonY } = this._layerPath(W, H, cameraX, layer)

      ctx.beginPath()
      ctx.moveTo(pts[0].x, pts[0].y)
      for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y)
      ctx.lineTo(W + this.cfg.step, H + 4)
      ctx.lineTo(-this.cfg.step, H + 4)
      ctx.closePath()

      // Vertical gradient: the peak tints pale with distance haze, the base
      // darkens toward the ground so the range reads as three-dimensional.
      const g = ctx.createLinearGradient(0, horizonY - layer.amplitude * H, 0, H)
      g.addColorStop(0, layer.peak)
      g.addColorStop(0.55, layer.color)
      g.addColorStop(1, layer.base)
      ctx.fillStyle = g
      ctx.fill()
    }
  }

  // Draw the backdrop. `cameraX` is the camera's world x; only its horizontal
  // component drives the parallax.
  draw(ctx, cameraX, W, H) {
    const px = this.cfg.pixelSize
    if (!px || px <= 1) {
      this._paint(ctx, W, H, cameraX)
      return
    }
    // Slightly pixelated: render the range to a small offscreen buffer and let
    // the browser scale it up with smoothing off, giving chunky edges cheaply.
    const bw = Math.max(2, Math.ceil(W / px))
    const bh = Math.max(2, Math.ceil(H / px))
    if (!this._buf) {
      this._buf = document.createElement('canvas')
      this._bufCtx = this._buf.getContext('2d')
    }
    if (this._buf.width !== bw || this._buf.height !== bh) {
      this._buf.width = bw
      this._buf.height = bh
    }
    const bctx = this._bufCtx
    bctx.setTransform(1, 0, 0, 1, 0, 0)
    bctx.clearRect(0, 0, bw, bh)
    bctx.scale(1 / px, 1 / px)
    this._paint(bctx, W, H, cameraX)

    ctx.save()
    ctx.imageSmoothingEnabled = false
    ctx.drawImage(this._buf, 0, 0, bw, bh, 0, 0, bw * px, bh * px)
    ctx.restore()
  }
}
