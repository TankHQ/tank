import { PALETTE, HULL_POINTS } from './config.js'
import { TAU, clamp, mix } from './util.js'

// Wedge turret outline in body-local pixels (y points up, matching the hull).
// Long and low, spanning most of the hull, with a hard straight top.
const TURRET_POINTS = [
  { x: -150, y: -52 },
  { x: -146, y: -92 },
  { x: -118, y: -116 },
  { x: 42, y: -116 },
  { x: 82, y: -106 },
  { x: 116, y: -82 },
  { x: 120, y: -52 },
]

// Where the gun axis leaves the turret. Kept in step with Tank.muzzle().
const GUN_Y = -88
const MUZZLE_X = 420

// Irregular desert-camo blotches, clipped to the hull / turret.
// Broad, smooth camouflage fields rather than small sharp facets, so the body
// reads as painted camouflage instead of low-poly shading.
const HULL_CAMO = [
  [[-184, -44], [-136, -50], [-96, -30], [-120, -10], [-172, -12]],
  [[-104, -48], [-44, -46], [-16, -24], [-70, -6], [-110, -20]],
  [[-34, -44], [40, -48], [66, -24], [16, -6], [-42, -18]],
  [[28, -46], [110, -40], [162, -18], [96, -4], [22, -18]],
  [[-190, -10], [-114, 2], [-58, -4], [-140, -26], [-194, -20]],
  [[-74, 0], [8, 10], [56, 0], [-2, -18], [-84, -16]],
  [[48, 2], [128, 10], [176, -10], [110, -20], [42, -14]],
]
const TURRET_CAMO = [
  [[-144, -74], [-104, -104], [-54, -86], [-92, -58], [-148, -62]],
  [[-58, -102], [14, -108], [58, -86], [-10, -68], [-64, -84]],
  [[2, -70], [92, -92], [112, -68], [46, -54]],
  [[-110, -56], [-36, -70], [-14, -56], [-90, -52]],
]
// Smoke-grenade discharger clusters on the turret sides (Abrams signature).
const SMOKE_ROWS = [
  { x: -112, y: -80, n: 6 },
  { x: -112, y: -68, n: 6 },
]

// Canvas 2D renderer.
//
// Everything is drawn from an interpolated `view` snapshot (see Tank.view), not
// from live physics state, so motion stays smooth above the 60 Hz physics rate.
// Vector art is used by default; drop body.png / wheel.png / turret.png into
// docs/public/game/ and they replace the drawn hull/wheel/turret.
export class Renderer {
  constructor(canvas, base) {
    this.canvas = canvas
    this.ctx = canvas.getContext('2d')
    this.base = base
    this.dpr = 1
    this.width = 0
    this.height = 0
    this.images = { body: null, wheel: null, turret: null }
    this._loadImage('body', 'game/body.png')
    this._loadImage('wheel', 'game/wheel.png')
    this._loadImage('turret', 'game/turret.png')
  }

  _loadImage(key, path) {
    const img = new Image()
    img.onload = () => { this.images[key] = img }
    img.onerror = () => { this.images[key] = null }
    img.src = this.base + path
  }

  resize(rect) {
    this.dpr = Math.min(window.devicePixelRatio || 1, 2)
    this.width = Math.max(320, Math.floor(rect.width))
    this.height = Math.max(280, Math.floor(rect.height))
    this.canvas.width = Math.floor(this.width * this.dpr)
    this.canvas.height = Math.floor(this.height * this.dpr)
    this.canvas.style.width = this.width + 'px'
    this.canvas.style.height = this.height + 'px'
  }

  // --- background -----------------------------------------------------------

  _drawSky(ctx) {
    const { width: W, height: H } = this
    const g = ctx.createLinearGradient(0, 0, 0, H)
    g.addColorStop(0, PALETTE.sky[0])
    g.addColorStop(0.42, PALETTE.sky[1])
    g.addColorStop(0.74, PALETTE.sky[2])
    g.addColorStop(1, PALETTE.sky[3])
    ctx.fillStyle = g
    ctx.fillRect(0, 0, W, H)

    const sunX = W * 0.74
    const sunY = H * 0.3
    const sg = ctx.createRadialGradient(sunX, sunY, 0, sunX, sunY, 260)
    sg.addColorStop(0, 'rgba(255,220,160,0.95)')
    sg.addColorStop(0.35, 'rgba(255,190,120,0.35)')
    sg.addColorStop(1, 'rgba(255,190,120,0)')
    ctx.fillStyle = sg
    ctx.fillRect(0, 0, W, H)

    ctx.beginPath()
    ctx.arc(sunX, sunY, 46, 0, TAU)
    ctx.fillStyle = PALETTE.sun
    ctx.fill()
  }

  _drawHills(ctx, cameraX, parallax, amplitude, baseY, color, frequency) {
    const { width: W, height: H } = this
    ctx.beginPath()
    ctx.moveTo(0, H)
    for (let x = 0; x <= W; x += 14) {
      const wx = cameraX * parallax + x
      const y = baseY + Math.sin(wx * frequency) * amplitude + Math.sin(wx * frequency * 2.3 + 1.1) * amplitude * 0.4
      ctx.lineTo(x, y)
    }
    ctx.lineTo(W, H)
    ctx.closePath()
    ctx.fillStyle = color
    ctx.fill()
  }

  // --- world ----------------------------------------------------------------

  _drawTerrain(ctx, groundY, cameraX, cameraY) {
    const { width: W, height: H } = this
    const x0 = cameraX - 40
    const x1 = cameraX + W + 40
    const surface = []

    ctx.beginPath()
    ctx.moveTo(x0, cameraY + H + 40)
    for (let wx = x0; wx <= x1; wx += 8) {
      const wy = groundY(wx)
      surface.push({ x: wx, y: wy })
      ctx.lineTo(wx, wy)
    }
    ctx.lineTo(x1, cameraY + H + 40)
    ctx.closePath()
    const g = ctx.createLinearGradient(0, cameraY + H * 0.4, 0, cameraY + H)
    g.addColorStop(0, PALETTE.soil)
    g.addColorStop(1, PALETTE.soilDeep)
    ctx.fillStyle = g
    ctx.fill()

    ctx.beginPath()
    ctx.moveTo(surface[0].x, surface[0].y)
    for (let i = 1; i < surface.length; i++) ctx.lineTo(surface[i].x, surface[i].y)
    ctx.lineWidth = 5
    ctx.strokeStyle = PALETTE.grassTop
    ctx.stroke()
    ctx.lineWidth = 2
    ctx.strokeStyle = PALETTE.grassDark
    ctx.stroke()
  }

  _path(ctx, points) {
    ctx.beginPath()
    ctx.moveTo(points[0].x, points[0].y)
    for (let i = 1; i < points.length; i++) ctx.lineTo(points[i].x, points[i].y)
    ctx.closePath()
  }

  // Closed smooth curve through camo control points (Catmull-Rom style), so the
  // blotches read as soft painted fields rather than sharp polygons.
  _blobPath(ctx, pts) {
    const n = pts.length
    ctx.beginPath()
    ctx.moveTo((pts[0].x + pts[n - 1].x) / 2, (pts[0].y + pts[n - 1].y) / 2)
    for (let i = 0; i < n; i++) {
      const p = pts[i]
      const q = pts[(i + 1) % n]
      ctx.quadraticCurveTo(p.x, p.y, (p.x + q.x) / 2, (p.y + q.y) / 2)
    }
    ctx.closePath()
  }

  // Clip to `clip`, paint the base gradient, then lay camo blotches on top.
  _drawCamoBody(ctx, clip, camo, yTop, yBottom) {
    ctx.save()
    this._path(ctx, clip)
    ctx.clip()
    const g = ctx.createLinearGradient(0, yTop, 0, yBottom)
    g.addColorStop(0, PALETTE.hull[0])
    g.addColorStop(0.6, PALETTE.hull[1])
    g.addColorStop(1, PALETTE.hull[2])
    ctx.fillStyle = g
    ctx.fillRect(-260, yTop - 60, 600, yBottom - yTop + 120)

    camo.forEach((blotch, i) => {
      this._blobPath(ctx, blotch.map(([x, y]) => ({ x, y })))
      ctx.fillStyle = PALETTE.camo[(i % (PALETTE.camo.length - 1)) + 1]
      ctx.globalAlpha = i % 2 ? 0.72 : 0.58
      ctx.fill()
    })
    ctx.globalAlpha = 1

    // Top highlight and lower shadow bands.
    ctx.fillStyle = 'rgba(255,250,235,0.18)'
    ctx.fillRect(-260, yTop + 1, 600, 4)
    ctx.fillStyle = 'rgba(60,40,18,0.28)'
    ctx.fillRect(-260, yBottom - 6, 600, 6)
    ctx.restore()
  }

  _drawHull(ctx, pose) {
    ctx.save()
    ctx.translate(pose.x, pose.y)
    ctx.rotate(pose.angle)
    if (this.images.body) {
      ctx.drawImage(this.images.body, -116, -28, 232, 56)
      ctx.restore()
      return
    }

    this._drawCamoBody(ctx, HULL_POINTS, HULL_CAMO, -50, 14)

    // Hull outline, with the deck edge picked out.
    this._path(ctx, HULL_POINTS)
    ctx.lineWidth = 2
    ctx.strokeStyle = PALETTE.hullLine
    ctx.stroke()

    // Side-skirt panel: a band from the skirt line down to the bottom, which
    // physically covers the top half of the idler wheels.
    const SKIRT_TOP = -14
    const SKIRT_BOTTOM = 14
    const skL = -180
    const skR = 176
    ctx.fillStyle = 'rgba(40,28,12,0.16)'
    ctx.fillRect(skL, SKIRT_TOP, skR - skL, SKIRT_BOTTOM - SKIRT_TOP)
    ctx.fillStyle = 'rgba(120,92,54,0.4)'
    ctx.fillRect(skL, SKIRT_TOP - 2, skR - skL, 4)
    ctx.strokeStyle = 'rgba(80,58,30,0.5)'
    ctx.lineWidth = 1
    ctx.strokeRect(skL, SKIRT_TOP - 2, skR - skL, 4)

    // Vertical panel divisions along the skirt (mudguards over each station).
    ctx.strokeStyle = 'rgba(80,58,30,0.4)'
    const panels = 8
    for (let i = 1; i < panels; i++) {
      const x = skL + (i * (skR - skL)) / panels
      ctx.beginPath()
      ctx.moveTo(x, SKIRT_TOP)
      ctx.lineTo(x, SKIRT_BOTTOM)
      ctx.stroke()
    }

    // Stowage bins and grab rails on the upper hull.
    for (const [bx, bw] of [[-96, 40], [44, 44]]) {
      ctx.fillStyle = 'rgba(90,68,36,0.3)'
      ctx.fillRect(bx, -42, bw, 20)
      ctx.strokeStyle = 'rgba(80,58,30,0.5)'
      ctx.strokeRect(bx, -42, bw, 20)
    }

    // Driver's hatch and a headlight at the front.
    ctx.beginPath()
    ctx.arc(-130, -30, 7, 0, TAU)
    ctx.fillStyle = 'rgba(70,52,26,0.55)'
    ctx.fill()
    ctx.beginPath()
    ctx.arc(152, -8, 3.6, 0, TAU)
    ctx.fillStyle = 'rgba(240,236,220,0.85)'
    ctx.fill()

    ctx.restore()
  }

  _drawTurret(ctx, pose) {
    ctx.save()
    ctx.translate(pose.x, pose.y)
    ctx.rotate(pose.angle)
    if (this.images.turret) {
      ctx.drawImage(this.images.turret, -60, -56, 120, 56)
      ctx.restore()
      return
    }

    // Main gun: long 120 mm barrel with a stepped thermal sleeve, fume
    // extractor bulge and a muzzle reference sensor at the tip.
    ctx.save()
    ctx.fillStyle = PALETTE.barrel
    // Breech / mantlet block where the gun meets the turret.
    ctx.fillRect(76, GUN_Y - 14, 52, 28)
    ctx.strokeStyle = PALETTE.hullLine
    ctx.lineWidth = 1
    ctx.strokeRect(76, GUN_Y - 14, 52, 28)
    // Barrel.
    ctx.fillStyle = PALETTE.barrel
    ctx.fillRect(118, GUN_Y - 6.5, MUZZLE_X - 118, 13)
    // Thermal-sleeve highlight along the top.
    ctx.fillStyle = PALETTE.barrelHi
    ctx.fillRect(124, GUN_Y - 6, MUZZLE_X - 156, 3)
    // Fume extractor bulge.
    ctx.fillStyle = PALETTE.barrel
    ctx.fillRect(MUZZLE_X - 92, GUN_Y - 11, 38, 22)
    ctx.strokeStyle = PALETTE.hullLine
    ctx.strokeRect(MUZZLE_X - 92, GUN_Y - 11, 38, 22)
    // Muzzle brake / reference sensor.
    ctx.fillStyle = PALETTE.barrel
    ctx.fillRect(MUZZLE_X - 28, GUN_Y - 9, 28, 18)
    ctx.strokeStyle = PALETTE.hullLine
    ctx.strokeRect(MUZZLE_X - 28, GUN_Y - 9, 28, 18)
    ctx.restore()

    // Wedge turret body in desert camo.
    this._drawCamoBody(ctx, TURRET_POINTS, TURRET_CAMO, -104, -48)
    this._path(ctx, TURRET_POINTS)
    ctx.lineWidth = 2
    ctx.strokeStyle = PALETTE.hullLine
    ctx.stroke()

    // Top deck line, picking out the flat roof.
    ctx.strokeStyle = 'rgba(80,58,30,0.4)'
    ctx.lineWidth = 1
    ctx.beginPath()
    ctx.moveTo(-94, -104)
    ctx.lineTo(94, -104)
    ctx.stroke()

    // Commander's cupola (with vision blocks) and loader's hatch on the roof.
    ctx.beginPath()
    ctx.arc(-62, -114, 18, 0, TAU)
    ctx.fillStyle = PALETTE.hull[1]
    ctx.fill()
    ctx.strokeStyle = PALETTE.hullLine
    ctx.stroke()
    ctx.fillStyle = 'rgba(50,36,18,0.55)'
    for (let i = 0; i < 6; i++) {
      const a = Math.PI + (i + 0.5) * (Math.PI / 6)
      ctx.beginPath()
      ctx.arc(-62 + Math.cos(a) * 11, -114 + Math.sin(a) * 11, 2.2, 0, TAU)
      ctx.fill()
    }
    ctx.beginPath()
    ctx.arc(2, -113, 12, 0, TAU)
    ctx.fillStyle = PALETTE.hull[1]
    ctx.fill()
    ctx.strokeStyle = PALETTE.hullLine
    ctx.stroke()

    // Smoke-grenade discharger clusters on the turret side.
    ctx.fillStyle = '#4a3a22'
    for (const row of SMOKE_ROWS) {
      for (let i = 0; i < row.n; i++) {
        ctx.beginPath()
        ctx.arc(row.x + i * 9, row.y, 2.8, 0, TAU)
        ctx.fill()
      }
    }

    // Rear stowage rack and the T-shaped wind sensor mast.
    ctx.fillStyle = 'rgba(90,68,36,0.55)'
    ctx.fillRect(80, -94, 24, 42)
    ctx.strokeStyle = '#6b5633'
    ctx.lineWidth = 2
    ctx.beginPath()
    ctx.moveTo(92, -100)
    ctx.lineTo(92, -116)
    ctx.moveTo(85, -116)
    ctx.lineTo(99, -116)
    ctx.stroke()

    // Antenna at the rear of the turret.
    ctx.strokeStyle = '#4a4a44'
    ctx.lineWidth = 1.5
    ctx.beginPath()
    ctx.moveTo(-80, -110)
    ctx.lineTo(-88, -148)
    ctx.stroke()

    ctx.restore()
  }

  _drawWheel(ctx, pos, angle, radius, isIdler) {
    ctx.save()
    ctx.translate(pos.x, pos.y)
    ctx.rotate(angle)
    if (this.images.wheel && !isIdler) {
      ctx.drawImage(this.images.wheel, -radius, -radius, radius * 2, radius * 2)
      ctx.restore()
      return
    }
    // Tyre/rim, then the road-wheel disc, then the red hub cap.
    ctx.beginPath()
    ctx.arc(0, 0, radius, 0, TAU)
    ctx.fillStyle = PALETTE.wheel
    ctx.fill()
    ctx.lineWidth = 2
    ctx.strokeStyle = PALETTE.hullLine
    ctx.stroke()

    ctx.beginPath()
    ctx.arc(0, 0, radius * 0.66, 0, TAU)
    ctx.fillStyle = PALETTE.wheelRim
    ctx.fill()

    // Radial lightening holes around the wheel.
    ctx.fillStyle = 'rgba(60,46,24,0.5)'
    for (let i = 0; i < 5; i++) {
      const a = (i / 5) * TAU + 0.3
      ctx.beginPath()
      ctx.arc(Math.cos(a) * radius * 0.42, Math.sin(a) * radius * 0.42, radius * 0.14, 0, TAU)
      ctx.fill()
    }

    ctx.beginPath()
    ctx.arc(0, 0, radius * 0.26, 0, TAU)
    ctx.fillStyle = PALETTE.wheelHub
    ctx.fill()
    ctx.lineWidth = 1.5
    ctx.strokeStyle = 'rgba(60,40,20,0.7)'
    ctx.stroke()
    ctx.restore()
  }

  _drawBelt(ctx, belt, phase) {
    const { loop, spacing, count } = belt
    const baseIndex = phase / spacing
    for (let k = 0; k < count; k++) {
      const fi = ((baseIndex + k) % count + count) % count
      const i0 = Math.floor(fi)
      const i1 = (i0 + 1) % count
      const t = fi - i0
      const p = { x: mix(loop[i0].x, loop[i1].x, t), y: mix(loop[i0].y, loop[i1].y, t) }
      const a = loop[(i0 - 1 + count) % count]
      const b = loop[(i0 + 2) % count]
      const angle = Math.atan2(b.y - a.y, b.x - a.x)

      ctx.save()
      ctx.translate(p.x, p.y)
      ctx.rotate(angle)
      const L = spacing * 1.28
      const T = 6.4
      ctx.beginPath()
      ctx.roundRect(-L / 2, -T / 2, L, T, 2)
      const g = ctx.createLinearGradient(0, -T / 2, 0, T / 2)
      g.addColorStop(0, PALETTE.trackHi)
      g.addColorStop(0.5, PALETTE.track)
      g.addColorStop(1, '#151412')
      ctx.fillStyle = g
      ctx.fill()
      ctx.strokeStyle = 'rgba(0,0,0,0.5)'
      ctx.lineWidth = 0.7
      ctx.stroke()
      ctx.beginPath()
      ctx.arc(L / 2 - 1.4, 0, 1.5, 0, TAU)
      ctx.fillStyle = '#0d0c0b'
      ctx.fill()
      ctx.beginPath()
      ctx.arc(-L / 2 + 1.4, 0, 1.5, 0, TAU)
      ctx.fillStyle = '#0d0c0b'
      ctx.fill()
      ctx.restore()
    }
  }

  _drawParticles(ctx, particles, shells) {
    for (const p of particles) {
      const a = clamp(p.life / p.max, 0, 1)
      ctx.globalAlpha = a * 0.85
      ctx.beginPath()
      ctx.arc(p.x, p.y, p.size * (1.4 - a * 0.4), 0, TAU)
      ctx.fillStyle = p.color
      ctx.fill()
    }
    ctx.globalAlpha = 1
    for (const shell of shells) {
      ctx.save()
      ctx.translate(shell.body.position.x, shell.body.position.y)
      ctx.rotate(shell.body.angle)
      ctx.fillStyle = '#d8c48a'
      ctx.fillRect(-5, -2, 10, 4)
      ctx.restore()
    }
  }

  // --- frame ----------------------------------------------------------------

  render({ camera, view, groundY, particles, shells }) {
    const ctx = this.ctx
    const { width: W, height: H } = this
    ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0)

    this._drawSky(ctx)
    this._drawHills(ctx, camera.x, 0.12, 30, H * 0.58, PALETTE.farHill, 0.0018)
    this._drawHills(ctx, camera.x, 0.28, 26, H * 0.68, PALETTE.midHill, 0.0026)
    this._drawHills(ctx, camera.x, 0.5, 20, H * 0.78, PALETTE.nearHill, 0.0034)

    const shake = camera.shakeOffset()
    ctx.save()
    ctx.translate(-camera.x + shake.x, -camera.y + shake.y)

    this._drawTerrain(ctx, groundY, camera.x, camera.y)

    if (view) {
      ctx.save()
      ctx.globalAlpha = 0.22
      ctx.beginPath()
      ctx.ellipse(view.pose.x, groundY(view.pose.x) + 4, 104, 12, 0, 0, TAU)
      ctx.fillStyle = '#000'
      ctx.fill()
      ctx.restore()

      // Running gear first: the side skirt (part of the hull) is drawn over it
      // so the top half of the wheels and the top run of the track are hidden.
      if (view.belt) this._drawBelt(ctx, view.belt, view.trackPhase)
      for (const idler of view.idlers) this._drawWheel(ctx, idler, idler.spin ?? 0, idler.r, true)
      for (const wheel of view.wheels) this._drawWheel(ctx, wheel, wheel.spinAngle, wheel.radius, false)
      this._drawHull(ctx, view.pose)
      this._drawTurret(ctx, view.pose)
    }
    this._drawParticles(ctx, particles, shells)
    ctx.restore()

    const vignette = ctx.createRadialGradient(W / 2, H / 2, H * 0.4, W / 2, H / 2, H * 0.95)
    vignette.addColorStop(0, 'rgba(0,0,0,0)')
    vignette.addColorStop(1, 'rgba(0,0,0,0.45)')
    ctx.fillStyle = vignette
    ctx.fillRect(0, 0, W, H)
  }
}
