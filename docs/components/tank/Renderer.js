import { PALETTE, HULL_ART, BODY, TURRET, GUN, CAMO, BODY_TILT } from './config.js'
import { TAU, clamp, mix } from './util.js'

// The drawable hull and turret outlines, straight from the reference SVG.
const HULL_POINTS = HULL_ART
const TURRET_POINTS = TURRET.points
const TURRET_ROOF = TURRET.roof

const GUN_Y = GUN.muzzleY
const GUN_ANCHOR_X = GUN.anchorX
const MUZZLE_X = GUN.muzzleX
const BARREL_R = GUN.radius

// Small helpers for drawing in the body-local frame (y points down).
const toPoints = (pts) => pts.map((p) => ({ x: p.x, y: p.y }))

// Rotate the body-local frame slightly nose-up about the rear of the hull. This
// is applied identically to the hull, turret and cannon so they move as one, and
// it only affects what is drawn — never the physics.
function applyBodyTilt(ctx) {
  ctx.translate(BODY_TILT.pivotX, BODY_TILT.pivotY)
  ctx.rotate(BODY_TILT.angle)
  ctx.translate(-BODY_TILT.pivotX, -BODY_TILT.pivotY)
}

// Draw a filled + stroked polygon from an array of [x, y] pairs.
function path(ctx, pts) {
  ctx.beginPath()
  ctx.moveTo(pts[0].x, pts[0].y)
  for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y)
  ctx.closePath()
}

// Canvas 2D renderer.
//
// Everything is drawn from an interpolated `view` snapshot (see Tank.view), not
// from live physics state, so motion stays smooth above the 60 Hz physics rate.
// The hull, turret and camouflage are vector art traced from the reference SVG;
// drop body.png / wheel.png / turret.png into docs/public/game/ to override the
// hull, wheels and turret with sprites.
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

  // --- hull and turret ------------------------------------------------------

  // Paint the hull base, then lay the reference camouflage fields over it. Both
  // are clipped to the hull outline so no field bleeds past the body.
  _drawHullArt(ctx) {
    ctx.save()
    path(ctx, HULL_POINTS)
    ctx.clip()

    const g = ctx.createLinearGradient(0, BODY.deckY, 0, BODY.skirtY)
    g.addColorStop(0, PALETTE.hull[0])
    g.addColorStop(0.6, PALETTE.hull[1])
    g.addColorStop(1, PALETTE.hull[2])
    ctx.fillStyle = g
    ctx.fillRect(BODY.minX - 40, BODY.deckY - 40, BODY.length + 80, BODY.depth + 80)

    // The reference camouflage fields, painted as flattened polygons in the
    // order the source SVG layers them.
    CAMO.forEach((field) => {
      path(ctx, toPoints(field.points))
      ctx.fillStyle = field.fill ?? '#bd9868'
      ctx.fill()
    })

    // Top highlight and lower shadow bands.
    ctx.fillStyle = 'rgba(255,250,235,0.16)'
    ctx.fillRect(BODY.minX - 40, BODY.deckY, BODY.length + 80, 4)
    ctx.fillStyle = 'rgba(60,40,18,0.24)'
    ctx.fillRect(BODY.minX - 40, BODY.skirtY - 6, BODY.length + 80, 6)
    ctx.restore()

    // Outline on top of the fill.
    path(ctx, HULL_POINTS)
    ctx.lineWidth = 2
    ctx.strokeStyle = PALETTE.hullLine
    ctx.stroke()
  }

  _drawHull(ctx, pose) {
    ctx.save()
    ctx.translate(pose.x, pose.y)
    ctx.rotate(pose.angle)
    applyBodyTilt(ctx)
    if (this.images.body) {
      ctx.drawImage(this.images.body, BODY.minX, BODY.deckY, BODY.length, BODY.depth)
      ctx.restore()
      return
    }
    this._drawHullArt(ctx)
    ctx.restore()
  }

  _drawTurret(ctx, pose) {
    ctx.save()
    ctx.translate(pose.x, pose.y)
    ctx.rotate(pose.angle)
    applyBodyTilt(ctx)
    const L = BODY.length
    if (this.images.turret) {
      ctx.drawImage(this.images.turret, BODY.minX + L * 0.06, TURRET_ROOF, L * 0.6, BODY.deckY - TURRET_ROOF)
      ctx.restore()
      return
    }

    // The cannon, grown forward from the turret's gun anchor. A tapered barrel
    // with a thermal sleeve, a fume extractor and a muzzle reference sensor.
    ctx.save()
    const sleeveEnd = GUN_ANCHOR_X + (MUZZLE_X - GUN_ANCHOR_X) * 0.72
    const feX = MUZZLE_X - BARREL_R * 5.0
    // Breech / mantlet block where the gun meets the turret.
    ctx.fillStyle = PALETTE.barrel
    ctx.fillRect(GUN_ANCHOR_X - BARREL_R * 0.4, GUN_Y - BARREL_R * 1.7, BARREL_R * 2.4, BARREL_R * 3.4)
    ctx.strokeStyle = PALETTE.hullLine
    ctx.lineWidth = 1
    ctx.strokeRect(GUN_ANCHOR_X - BARREL_R * 0.4, GUN_Y - BARREL_R * 1.7, BARREL_R * 2.4, BARREL_R * 3.4)
    // Barrel.
    ctx.fillStyle = PALETTE.barrel
    ctx.fillRect(GUN_ANCHOR_X + BARREL_R * 1.8, GUN_Y - BARREL_R, sleeveEnd - GUN_ANCHOR_X - BARREL_R * 1.8, BARREL_R * 2)
    ctx.fillRect(sleeveEnd - BARREL_R * 0.4, GUN_Y - BARREL_R * 0.72, MUZZLE_X - sleeveEnd + BARREL_R * 0.4, BARREL_R * 1.44)
    // Thermal-sleeve highlight.
    ctx.fillStyle = PALETTE.barrelHi
    ctx.fillRect(GUN_ANCHOR_X + BARREL_R * 2.2, GUN_Y - BARREL_R * 0.7, sleeveEnd - GUN_ANCHOR_X - BARREL_R * 3.4, BARREL_R * 0.4)
    // Fume extractor bulge.
    ctx.fillStyle = PALETTE.barrel
    ctx.fillRect(feX, GUN_Y - BARREL_R * 1.5, BARREL_R * 3.6, BARREL_R * 3)
    ctx.strokeRect(feX, GUN_Y - BARREL_R * 1.5, BARREL_R * 3.6, BARREL_R * 3)
    // Muzzle brake / reference sensor.
    ctx.fillRect(MUZZLE_X - BARREL_R * 1.6, GUN_Y - BARREL_R * 1.25, BARREL_R * 1.6, BARREL_R * 2.5)
    ctx.strokeRect(MUZZLE_X - BARREL_R * 1.6, GUN_Y - BARREL_R * 1.25, BARREL_R * 1.6, BARREL_R * 2.5)
    ctx.restore()

    // Turret wedge, camo-painted like the hull. The source outline already
    // carries the roof details (cupola and hatch steps).
    ctx.save()
    path(ctx, TURRET_POINTS)
    ctx.clip()
    const tg = ctx.createLinearGradient(0, TURRET_ROOF, 0, BODY.deckY)
    tg.addColorStop(0, PALETTE.hull[0])
    tg.addColorStop(1, PALETTE.hull[1])
    ctx.fillStyle = tg
    ctx.fillRect(BODY.minX, TURRET_ROOF - 20, BODY.length + 20, BODY.deckY - TURRET_ROOF + 40)
    ctx.restore()

    path(ctx, TURRET_POINTS)
    ctx.lineWidth = 2
    ctx.strokeStyle = PALETTE.hullLine
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

    // Tyre/rim.
    ctx.beginPath()
    ctx.arc(0, 0, radius, 0, TAU)
    ctx.fillStyle = PALETTE.wheel
    ctx.fill()
    ctx.lineWidth = 2
    ctx.strokeStyle = PALETTE.hullLine
    ctx.stroke()

    if (isIdler) {
      // Return idlers read as spoked discs: a rim, spokes and a small hub, with
      // no red cap, so they are clearly the "top wheels" of the reference.
      ctx.beginPath()
      ctx.arc(0, 0, radius * 0.82, 0, TAU)
      ctx.fillStyle = PALETTE.wheelRim
      ctx.fill()
      ctx.strokeStyle = 'rgba(80,58,30,0.5)'
      ctx.lineWidth = 1
      for (let i = 0; i < 8; i++) {
        const a = (i / 8) * TAU
        ctx.beginPath()
        ctx.moveTo(Math.cos(a) * radius * 0.24, Math.sin(a) * radius * 0.24)
        ctx.lineTo(Math.cos(a) * radius * 0.78, Math.sin(a) * radius * 0.78)
        ctx.stroke()
      }
      ctx.beginPath()
      ctx.arc(0, 0, radius * 0.24, 0, TAU)
      ctx.fillStyle = PALETTE.wheel
      ctx.fill()
      ctx.strokeStyle = PALETTE.hullLine
      ctx.stroke()
      ctx.restore()
      return
    }

    // Road-wheel disc, then the red hub cap.
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
      const T = spacing * 0.75
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
      ctx.arc(L / 2 - T * 0.22, 0, T * 0.23, 0, TAU)
      ctx.fillStyle = '#0d0c0b'
      ctx.fill()
      ctx.beginPath()
      ctx.arc(-L / 2 + T * 0.22, 0, T * 0.23, 0, TAU)
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
      ctx.ellipse(view.pose.x, groundY(view.pose.x) + 4, BODY.length * 0.186, BODY.length * 0.021, 0, 0, TAU)
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
