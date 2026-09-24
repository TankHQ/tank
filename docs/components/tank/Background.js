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
// The same heights can be rasterised several ways; `cfg.style` picks one:
//
//   'smooth'  - sampled densely and filled, reading as soft ridged rock
//   'lowpoly' - sampled coarsely and shaded per facet, reading as sharp,
//               stylised faceted peaks
//   'outline' - filled flat, then the ridge is drawn as a bold stylised line
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
  // `step` controls the horizontal sampling: fine for smooth, coarse for facets.
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

  // Trace a filled silhouette from a height line, closing down to the ground.
  _fillSilhouette(ctx, pts, W, H) {
    ctx.beginPath()
    ctx.moveTo(pts[0].x, pts[0].y)
    for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y)
    ctx.lineTo(pts[pts.length - 1].x, H + 4)
    ctx.lineTo(pts[0].x, H + 4)
    ctx.closePath()
  }

  _layerGradient(ctx, layer, horizonY, H) {
    const g = ctx.createLinearGradient(0, horizonY - layer.amplitude * H, 0, H)
    g.addColorStop(0, layer.peak)
    g.addColorStop(0.55, layer.color)
    g.addColorStop(1, layer.base)
    return g
  }

  // Dense sampling, filled with the vertical haze gradient: soft ridges.
  _paintSmooth(ctx, W, H, cameraX, layer, horizonY) {
    const { pts } = this._layerPath(W, H, cameraX, layer, this.cfg.step)
    this._fillSilhouette(ctx, pts, W, H)
    ctx.fillStyle = this._layerGradient(ctx, layer, horizonY, H)
    ctx.fill()
  }

  // Coarse sampling shaded one facet at a time. Triangles are built from each
  // ridge point down to the ground, and each is shaded by the local slope, so
  // faces turned away from the light go dark and the range reads as sharp
  // stylised facets rather than a smooth silhouette.
  _paintLowpoly(ctx, W, H, cameraX, layer, horizonY) {
    const step = layer.facet ?? this.cfg.facetStep
    const { pts } = this._layerPath(W, H, cameraX, layer, step)
    const { lightDir, facetContrast } = this.cfg

    // A flat base under the facets, so no background shows between triangles.
    this._fillSilhouette(ctx, pts, W, H)
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
      // sink toward `base`, so a ridge has a clear bright and dark side.
      const g = ctx.createLinearGradient(0, Math.min(a.y, b.y), 0, H)
      g.addColorStop(0, mixColor(layer.base, layer.peak, t))
      g.addColorStop(1, mixColor(layer.base, layer.color, t * 0.5))
      ctx.fillStyle = g
      ctx.fill()
    }
  }

  // Flat fill plus a bold stroke along the ridge line, and a fainter parallel
  // line offset inward, giving stylised line-art peaks.
  _paintOutline(ctx, W, H, cameraX, layer, horizonY) {
    const { pts } = this._layerPath(W, H, cameraX, layer, this.cfg.step)
    this._fillSilhouette(ctx, pts, W, H)
    const g = ctx.createLinearGradient(0, horizonY - layer.amplitude * H, 0, H)
    g.addColorStop(0, layer.peak)
    g.addColorStop(1, layer.base)
    ctx.fillStyle = g
    ctx.fill()

    ctx.lineJoin = 'round'
    ctx.lineCap = 'round'
    const stroke = (offset, width, color) => {
      ctx.beginPath()
      ctx.moveTo(pts[0].x, pts[0].y + offset)
      for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y + offset)
      ctx.lineWidth = width
      ctx.strokeStyle = color
      ctx.stroke()
    }
    // Crisp ridge crease, then a fainter contour a little way below it.
    stroke(0, this.cfg.outlineWidth, this.cfg.outlineColor)
    stroke(this.cfg.outlineWidth * 3.5, this.cfg.outlineWidth * 0.7, this.cfg.outlineColor2)
  }

  _paintLayer(ctx, W, H, cameraX, layer, horizonY) {
    switch (this.cfg.style) {
      case 'lowpoly': return this._paintLowpoly(ctx, W, H, cameraX, layer, horizonY)
      case 'outline': return this._paintOutline(ctx, W, H, cameraX, layer, horizonY)
      default: return this._paintSmooth(ctx, W, H, cameraX, layer, horizonY)
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
