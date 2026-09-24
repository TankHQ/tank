import { valueNoise } from './util.js'

// Mix two #rrggbb colours; t=0 gives a, t=1 gives b. Used to shade facets.
function mixColor(a, b, t) {
  const pa = parseInt(a.slice(1), 16)
  const pb = parseInt(b.slice(1), 16)
  const ar = (pa >> 16) & 255, ag = (pa >> 8) & 255, ab = pa & 255
  const br = (pb >> 16) & 255, bg = (pb >> 8) & 255, bb = pb & 255
  const r = Math.round(ar + (br - ar) * t)
  const g = Math.round(ag + (bg - ag) * t)
  const bl = Math.round(ab + (bb - ab) * t)
  return `rgb(${r},${g},${bl})`
}

// A layered, procedurally generated mountain backdrop with a sharp, faceted
// look.
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
// To get the sharp look the height line is sampled coarsely and each segment is
// drawn as a facet shaded by its slope, so faces leaning away from the light go
// dark and faces leaning toward it go bright. The result is hard, stylised
// peaks rather than a smooth silhouette.
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
  // `step` controls the horizontal sampling: coarse sampling is what gives the
  // facets their size.
  _layerPath(W, H, cameraX, layer, step) {
    const horizonY = H * this.cfg.horizon
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

  // One layer, sampled coarsely and shaded per facet.
  _paintLayer(ctx, W, H, cameraX, layer, horizonY) {
    const { pts } = this._layerPath(W, H, cameraX, layer, this.cfg.facetStep)
    const { lightDir, facetContrast } = this.cfg

    // A flat base under the facets, so no background shows between them.
    ctx.beginPath()
    ctx.moveTo(pts[0].x, pts[0].y)
    for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y)
    ctx.lineTo(pts[pts.length - 1].x, H + 4)
    ctx.lineTo(pts[0].x, H + 4)
    ctx.closePath()
    ctx.fillStyle = layer.color
    ctx.fill()

    for (let i = 0; i < pts.length - 1; i++) {
      const a = pts[i]
      const b = pts[i + 1]
      const slope = (b.y - a.y) / (b.x - a.x)
      // Light from one side: slopes leaning toward it read as lit faces, the
      // others as shadowed. `facetContrast` sets how far the two tones split.
      const lit = Math.max(0, Math.min(1, 0.5 - slope * lightDir))
      const t = 0.5 + (lit - 0.5) * (1 + facetContrast)
      ctx.beginPath()
      ctx.moveTo(a.x, a.y)
      ctx.lineTo(b.x, b.y)
      ctx.lineTo(b.x, H + 4)
      ctx.lineTo(a.x, H + 4)
      ctx.closePath()
      // Shade across the facet: lit faces climb toward `peak`, shadowed ones
      // sink toward `base`. The gradient lands on a common `base` at the
      // bottom, so the facets read as lit and shadowed slopes rather than as
      // separate vertical bars.
      const g = ctx.createLinearGradient(0, Math.min(a.y, b.y), 0, H)
      g.addColorStop(0, mixColor(layer.base, layer.peak, t))
      g.addColorStop(1, layer.base)
      ctx.fillStyle = g
      ctx.fill()
    }
  }

  _paint(ctx, W, H, cameraX) {
    const horizonY = H * this.cfg.horizon
    for (const layer of this.cfg.layers) {
      this._paintLayer(ctx, W, H, cameraX, layer, horizonY)
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
