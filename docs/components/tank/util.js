// Small math helpers used across the simulation.
//
// Scalar helpers live here because planck's own `Math` module only covers a few
// cases (`Math.clamp`, `Math.mod`); vector/transform maths is delegated to
// planck's `Vec2`/`Transform`/`Sweep` at the call sites instead.

export const TAU = Math.PI * 2

export const clamp = (value, min, max) => (value < min ? min : value > max ? max : value)

export const mix = (a, b, t) => a + (b - a) * t

export const distance = (a, b) => Math.hypot(a.x - b.x, a.y - b.y)

// Convex hull of a point cloud (Andrew's monotone chain, counter-clockwise).
// planck's PolygonShape also computes a hull, but it caps polygons at
// `Settings.maxPolygonVertices` (12), which is too few for the track belt.
export function convexHull(points) {
  if (points.length < 3) return points.slice()
  const sorted = points.slice().sort((a, b) => a.x - b.x || a.y - b.y)
  const cross = (o, a, b) => (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x)
  const build = (pts) => {
    const chain = []
    for (const p of pts) {
      while (chain.length >= 2 && cross(chain[chain.length - 2], chain[chain.length - 1], p) <= 0) chain.pop()
      chain.push(p)
    }
    return chain
  }
  const lower = build(sorted)
  const upper = build(sorted.slice().reverse())
  lower.pop()
  upper.pop()
  return lower.concat(upper)
}

// Deterministic 1-D value noise, used by the terrain generator.
function hash(n) {
  const x = Math.sin(n * 127.1 + 311.7) * 43758.5453123
  return x - Math.floor(x)
}

export function valueNoise(x) {
  const i = Math.floor(x)
  const f = x - i
  const u = f * f * (3 - 2 * f)
  return mix(hash(i), hash(i + 1), u)
}
