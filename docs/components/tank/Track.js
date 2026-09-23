import { TAU, distance, convexHull } from './util.js'

// The track belt that wraps the wheels.
//
// The belt is not simulated as bodies or constraints. Each frame its shape is
// rebuilt as the convex hull of every wheel and idler disc (inflated by a small
// clearance). Sampling that hull at a fixed arc spacing yields many short
// segments, so the belt drapes around each wheel and bends smoothly like a
// chain. The pads then scroll around the loop at ground speed.
export class Track {
  constructor({ planck, trackConfig }) {
    this._Math = planck.Math
    this._track = trackConfig
    this.phase = 0
  }

  // Convex hull of the inflated discs. Returns null if there are too few points
  // to form a loop.
  _outline(discs) {
    const points = []
    for (const disc of discs) {
      const r = disc.r + this._track.clearance
      const n = disc.r > 16 ? 22 : 18
      for (let i = 0; i < n; i++) {
        const a = (i / n) * TAU
        points.push({ x: disc.x + Math.cos(a) * r, y: disc.y + Math.sin(a) * r })
      }
    }
    return convexHull(points)
  }

  // Resample the hull at a fixed arc spacing to get evenly spaced pads, and
  // work out which way the loop flows (so scrolling pushes the right way).
  // `discs` is a list of { x, y, r } in world space.
  buildLoop(discs) {
    const hull = this._outline(discs)
    if (!hull || hull.length < 4) return null

    const n = hull.length
    const edgeLengths = []
    let total = 0
    for (let i = 0; i < n; i++) {
      const len = distance(hull[i], hull[(i + 1) % n])
      edgeLengths.push(len)
      total += len
    }

    const count = Math.max(this._track.minPads, Math.round(total / this._track.padLength))
    const spacing = total / count

    // Cumulative arc length at each vertex, for locating a pad by arc position.
    const cumulative = [0]
    for (let i = 0; i < n; i++) cumulative.push(cumulative[i] + edgeLengths[i])

    const loop = []
    for (let k = 0; k < count; k++) {
      const target = k * spacing
      let seg = 0
      while (seg < n - 1 && cumulative[seg + 1] < target) seg++
      const segLength = cumulative[seg + 1] - cumulative[seg] || 1
      const t = (target - cumulative[seg]) / segLength
      const a = hull[seg]
      const b = hull[(seg + 1) % n]
      loop.push({ x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t })
    }

    // Lowest point on the loop: the tangent there tells us the flow direction,
    // so the belt scrolls the same way the ground moves underneath.
    let lowest = 0
    for (let i = 1; i < loop.length; i++) if (loop[i].y > loop[lowest].y) lowest = i
    const before = loop[(lowest - 1 + loop.length) % loop.length]
    const after = loop[(lowest + 1) % loop.length]
    const flowSign = after.x - before.x < 0 ? 1 : -1

    return { loop, spacing, count, flowSign }
  }

  // Scroll the belt according to ground speed. `dt` is wall-clock time for this
  // rendered frame, so the scroll speed is refresh-rate independent.
  scroll(loop, groundSpeed, dt) {
    if (!loop) return
    const period = loop.spacing * loop.count
    this.phase += loop.flowSign * groundSpeed * this._track.scrollRate * (dt / 1000)
    this.phase = this._Math.mod(this.phase, period)
  }

  reset() {
    this.phase = 0
  }
}
