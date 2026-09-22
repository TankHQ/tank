// Canvas renderer. Vector art by default; drop PNG/SVG parts into
// docs/public/game/ (body.png, wheel.png, turret.png) and they are used
// automatically instead of the drawn hull/wheel/turret.

import { HULL_PTS } from './tank.js'

const TAU = Math.PI * 2
const clamp = (v, a, b) => (v < a ? a : v > b ? b : v)
const lerp = (a, b, t) => a + (b - a) * t

const palette = {
  sky: ['#1d2b40', '#3d4f68', '#9a6b46', '#e8b878'],
  sun: '#ffd9a0',
  farHill: '#4c5566',
  midHill: '#3f4a3f',
  nearHill: '#333a2f',
  grassTop: '#8f8155',
  grassDark: '#6b603c',
  soil: '#4a4029',
  soilDeep: '#332c1c',
  track: '#2a2825',
  trackHi: '#545049',
  hull: ['#6d6a4a', '#575339', '#3a3726'],
  wheel: '#26241f',
  wheelRim: '#4e4a40',
}

export function createRenderer(canvas, base) {
  const ctx = canvas.getContext('2d')
  let dpr = 1
  let W = 0
  let H = 0

  const IMG = { body: null, wheel: null, turret: null }

  function loadImage(key, path) {
    const img = new Image()
    img.onload = () => {
      IMG[key] = img
    }
    img.onerror = () => {
      IMG[key] = null
    }
    img.src = base + path
  }
  loadImage('body', 'game/body.png')
  loadImage('wheel', 'game/wheel.png')
  loadImage('turret', 'game/turret.png')

  function resize(rect) {
    dpr = Math.min(window.devicePixelRatio || 1, 2)
    W = Math.max(320, Math.floor(rect.width))
    H = Math.max(280, Math.floor(rect.height))
    canvas.width = Math.floor(W * dpr)
    canvas.height = Math.floor(H * dpr)
    canvas.style.width = W + 'px'
    canvas.style.height = H + 'px'
  }

  function drawSky() {
    const g = ctx.createLinearGradient(0, 0, 0, H)
    g.addColorStop(0, palette.sky[0])
    g.addColorStop(0.42, palette.sky[1])
    g.addColorStop(0.74, palette.sky[2])
    g.addColorStop(1, palette.sky[3])
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
    ctx.fillStyle = palette.sun
    ctx.fill()
  }

  function hills(parallax, amp, baseY, color, freq) {
    ctx.beginPath()
    ctx.moveTo(0, H)
    for (let x = 0; x <= W; x += 14) {
      const wx = camX * parallax + x
      const y = baseY + Math.sin(wx * freq) * amp + Math.sin(wx * freq * 2.3 + 1.1) * amp * 0.4
      ctx.lineTo(x, y)
    }
    ctx.lineTo(W, H)
    ctx.closePath()
    ctx.fillStyle = color
    ctx.fill()
  }

  let camX = 0
  let camY = 0

  function drawTerrainSurface(groundY) {
    const step = 8
    const x0 = camX - 40
    const x1 = camX + W + 40
    ctx.beginPath()
    ctx.moveTo(x0, camY + H + 40)
    const surf = []
    for (let wx = x0; wx <= x1; wx += step) {
      const wy = groundY(wx)
      surf.push({ x: wx, y: wy })
      ctx.lineTo(wx, wy)
    }
    ctx.lineTo(x1, camY + H + 40)
    ctx.closePath()
    const g = ctx.createLinearGradient(0, camY + H * 0.4, 0, camY + H)
    g.addColorStop(0, palette.soil)
    g.addColorStop(1, palette.soilDeep)
    ctx.fillStyle = g
    ctx.fill()

    ctx.beginPath()
    ctx.moveTo(surf[0].x, surf[0].y)
    for (let i = 1; i < surf.length; i++) ctx.lineTo(surf[i].x, surf[i].y)
    ctx.lineWidth = 5
    ctx.strokeStyle = palette.grassTop
    ctx.stroke()
    ctx.lineWidth = 2
    ctx.strokeStyle = palette.grassDark
    ctx.stroke()
  }

  function drawHull(hull) {
    ctx.save()
    ctx.translate(hull.position.x, hull.position.y)
    ctx.rotate(hull.angle)
    if (IMG.body) {
      ctx.drawImage(IMG.body, -116, -28, 232, 56)
    } else {
      ctx.beginPath()
      ctx.moveTo(HULL_PTS[0].x, HULL_PTS[0].y)
      for (let i = 1; i < HULL_PTS.length; i++) ctx.lineTo(HULL_PTS[i].x, HULL_PTS[i].y)
      ctx.closePath()
      const g = ctx.createLinearGradient(0, -26, 0, 14)
      g.addColorStop(0, palette.hull[0])
      g.addColorStop(0.55, palette.hull[1])
      g.addColorStop(1, palette.hull[2])
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
    }
    ctx.restore()
  }

  function drawTurret(hull) {
    if (!IMG.turret) return
    ctx.save()
    ctx.translate(hull.position.x, hull.position.y)
    ctx.rotate(hull.angle)
    ctx.drawImage(IMG.turret, -60, -56, 120, 56)
    ctx.restore()
  }

  function drawWheel(pos, angle, r, isIdler) {
    ctx.save()
    ctx.translate(pos.x, pos.y)
    ctx.rotate(angle)
    if (IMG.wheel && !isIdler) {
      ctx.drawImage(IMG.wheel, -r, -r, r * 2, r * 2)
    } else {
      ctx.beginPath()
      ctx.arc(0, 0, r, 0, TAU)
      ctx.fillStyle = palette.wheel
      ctx.fill()
      ctx.lineWidth = 1.5
      ctx.strokeStyle = 'rgba(0,0,0,0.6)'
      ctx.stroke()

      ctx.beginPath()
      ctx.arc(0, 0, r * 0.62, 0, TAU)
      ctx.fillStyle = palette.wheelRim
      ctx.fill()

      ctx.strokeStyle = 'rgba(20,18,14,0.9)'
      ctx.lineWidth = 2
      for (let i = 0; i < 6; i++) {
        const a = (i / 6) * TAU
        ctx.beginPath()
        ctx.moveTo(Math.cos(a) * r * 0.15, Math.sin(a) * r * 0.15)
        ctx.lineTo(Math.cos(a) * r * 0.6, Math.sin(a) * r * 0.6)
        ctx.stroke()
      }
      ctx.beginPath()
      ctx.arc(0, 0, r * 0.18, 0, TAU)
      ctx.fillStyle = '#8a8272'
      ctx.fill()
    }
    ctx.restore()
  }

  function drawBelt(belt, phase) {
    const { loop, spacing, count } = belt
    const baseIdx = phase / spacing
    for (let k = 0; k < count; k++) {
      const fi = ((baseIdx + k) % count + count) % count
      const i0 = Math.floor(fi)
      const i1 = (i0 + 1) % count
      const t = fi - i0
      const p = { x: lerp(loop[i0].x, loop[i1].x, t), y: lerp(loop[i0].y, loop[i1].y, t) }
      const a = loop[(i0 - 1 + count) % count]
      const b = loop[(i0 + 2) % count]
      const ang = Math.atan2(b.y - a.y, b.x - a.x)
      ctx.save()
      ctx.translate(p.x, p.y)
      ctx.rotate(ang)
      const L = spacing * 1.28
      const T = 6.4
      ctx.beginPath()
      ctx.roundRect(-L / 2, -T / 2, L, T, 2)
      const g = ctx.createLinearGradient(0, -T / 2, 0, T / 2)
      g.addColorStop(0, palette.trackHi)
      g.addColorStop(0.5, palette.track)
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

  function drawParticles(particles, shells) {
    for (const p of particles) {
      const a = clamp(p.life / p.max, 0, 1)
      ctx.globalAlpha = a * 0.85
      ctx.beginPath()
      ctx.arc(p.x, p.y, p.size * (1.4 - a * 0.4), 0, TAU)
      ctx.fillStyle = p.color
      ctx.fill()
    }
    ctx.globalAlpha = 1
    for (const s of shells) {
      ctx.save()
      ctx.translate(s.body.position.x, s.body.position.y)
      ctx.rotate(s.body.angle)
      ctx.fillStyle = '#d8c48a'
      ctx.fillRect(-5, -2, 10, 4)
      ctx.restore()
    }
  }

  function render(state) {
    const { cam, shake, tank, particles, shells, groundY } = state
    camX = cam.x
    camY = cam.y

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    drawSky()
    hills(0.12, 30, H * 0.58, palette.farHill, 0.0018)
    hills(0.28, 26, H * 0.68, palette.midHill, 0.0026)
    hills(0.5, 20, H * 0.78, palette.nearHill, 0.0034)

    const sx = (Math.random() - 0.5) * shake
    const sy = (Math.random() - 0.5) * shake
    ctx.save()
    ctx.translate(-cam.x + sx, -cam.y + sy)

    drawTerrainSurface(groundY)

    if (tank.hull) {
      ctx.save()
      ctx.globalAlpha = 0.22
      ctx.beginPath()
      ctx.ellipse(tank.hull.position.x, groundY(tank.hull.position.x) + 4, 104, 12, 0, 0, TAU)
      ctx.fillStyle = '#000'
      ctx.fill()
      ctx.restore()
    }

    if (tank.hull) {
      drawHull(tank.hull)
      drawTurret(tank.hull)
      for (const i of tank.idlers) drawWheel(i, 0, i.r, true)
      for (const w of tank.wheelPositions) drawWheel(w, w.spinAngle, w.radius, false)
      if (tank.belt) drawBelt(tank.belt, tank.trackPhase)
    }
    drawParticles(particles, shells)

    ctx.restore()

    const vg = ctx.createRadialGradient(W / 2, H / 2, H * 0.4, W / 2, H / 2, H * 0.95)
    vg.addColorStop(0, 'rgba(0,0,0,0)')
    vg.addColorStop(1, 'rgba(0,0,0,0.45)')
    ctx.fillStyle = vg
    ctx.fillRect(0, 0, W, H)
  }

  return { resize, render, get width() { return W }, get height() { return H } }
}
