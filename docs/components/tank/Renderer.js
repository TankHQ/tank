import { PALETTE, HULL_POINTS } from './config.js'
import { TAU, clamp, mix } from './util.js'

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

  _drawHull(ctx, pose) {
    ctx.save()
    ctx.translate(pose.x, pose.y)
    ctx.rotate(pose.angle)
    if (this.images.body) {
      ctx.drawImage(this.images.body, -116, -28, 232, 56)
      ctx.restore()
      return
    }

    ctx.beginPath()
    ctx.moveTo(HULL_POINTS[0].x, HULL_POINTS[0].y)
    for (let i = 1; i < HULL_POINTS.length; i++) ctx.lineTo(HULL_POINTS[i].x, HULL_POINTS[i].y)
    ctx.closePath()
    const g = ctx.createLinearGradient(0, -26, 0, 14)
    g.addColorStop(0, PALETTE.hull[0])
    g.addColorStop(0.55, PALETTE.hull[1])
    g.addColorStop(1, PALETTE.hull[2])
    ctx.fillStyle = g
    ctx.fill()
    ctx.lineWidth = 2
    ctx.strokeStyle = 'rgba(0,0,0,0.5)'
    ctx.stroke()

    ctx.fillStyle = 'rgba(255,255,255,0.08)'
    ctx.fillRect(-52, -23, 96, 5)
    ctx.fillStyle = 'rgba(0,0,0,0.28)'
    ctx.fillRect(-102, -1, 204, 5)
    ctx.beginPath()
    ctx.arc(-84, -16, 4, 0, TAU)
    ctx.fillStyle = '#e8d9a8'
    ctx.fill()

    ctx.fillStyle = 'rgba(0,0,0,0.35)'
    ctx.fillRect(46, -23, 26, 5)
    ctx.fillRect(78, -21, 10, 16)

    ctx.save()
    ctx.translate(0, -9)
    ctx.beginPath()
    for (let i = 0; i < 5; i++) {
      const a = -Math.PI / 2 + (i * TAU) / 5
      const x = Math.cos(a) * 12
      const y = Math.sin(a) * 12
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y)
    }
    ctx.closePath()
    ctx.fillStyle = 'rgba(191,177,145,0.55)'
    ctx.fill()
    ctx.restore()
    ctx.restore()
  }

  _drawTurret(ctx, pose) {
    if (!this.images.turret) return
    ctx.save()
    ctx.translate(pose.x, pose.y)
    ctx.rotate(pose.angle)
    ctx.drawImage(this.images.turret, -60, -56, 120, 56)
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
    ctx.beginPath()
    ctx.arc(0, 0, radius, 0, TAU)
    ctx.fillStyle = PALETTE.wheel
    ctx.fill()
    ctx.lineWidth = 1.5
    ctx.strokeStyle = 'rgba(0,0,0,0.6)'
    ctx.stroke()

    ctx.beginPath()
    ctx.arc(0, 0, radius * 0.62, 0, TAU)
    ctx.fillStyle = PALETTE.wheelRim
    ctx.fill()

    ctx.strokeStyle = 'rgba(20,18,14,0.9)'
    ctx.lineWidth = 2
    for (let i = 0; i < 6; i++) {
      const a = (i / 6) * TAU
      ctx.beginPath()
      ctx.moveTo(Math.cos(a) * radius * 0.15, Math.sin(a) * radius * 0.15)
      ctx.lineTo(Math.cos(a) * radius * 0.6, Math.sin(a) * radius * 0.6)
      ctx.stroke()
    }
    ctx.beginPath()
    ctx.arc(0, 0, radius * 0.18, 0, TAU)
    ctx.fillStyle = '#8a8272'
    ctx.fill()
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

      this._drawHull(ctx, view.pose)
      this._drawTurret(ctx, view.pose)
      for (const idler of view.idlers) this._drawWheel(ctx, idler, 0, idler.r, true)
      for (const wheel of view.wheels) this._drawWheel(ctx, wheel, wheel.spinAngle, wheel.radius, false)
      if (view.belt) this._drawBelt(ctx, view.belt, view.trackPhase)
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
