import { TAU, distance } from './util.js'

export class Track {
    constructor({ Matter, trackConfig }) {
        this._Vertices = Matter.Vertices
        this._track = trackConfig
        this.phase = 0 // Arc-length scroll offset along the belt (px)
    }

    // Convex hull of wheel/idler discs inflated by track clearance (px).
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
        return this._Vertices.hull(points)
    }

    buildLoop(discs, groundY) {
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
            const p = { x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t }
            // Clamp pads above terrain surface (world px, y down).
            if (groundY) {
                const surface = groundY(p.x)
                if (p.y > surface) p.y = surface
            }
            loop.push(p)
        }

        // Determine loop winding sign (+1 or -1) from tangent at lowest vertex.
        let lowest = 0
        for (let i = 1; i < loop.length; i++) if (loop[i].y > loop[lowest].y) lowest = i
        const before = loop[(lowest - 1 + loop.length) % loop.length]
        const after = loop[(lowest + 1) % loop.length]
        const flowSign = after.x - before.x < 0 ? 1 : -1

        return { loop, spacing, count, flowSign }
    }

    // Advance belt phase (px) from groundSpeed (px/frame) and frame dt (ms).
    scroll(loop, groundSpeed, dt) {
        if (!loop) return
        const period = loop.spacing * loop.count
        this.phase += loop.flowSign * groundSpeed * this._track.scrollRate * (dt / 1000)
        this.phase = ((this.phase % period) + period) % period
    }

    reset() {
        this.phase = 0
    }
}
