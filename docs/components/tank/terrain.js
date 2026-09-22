// Procedural, infinite-ish terrain built from static chain segments.
// The height function is shared by physics and rendering so the drawn
// surface is exactly what the wheels collide with.

const BASE_Y = 360

const clamp = (v, a, b) => (v < a ? a : v > b ? b : v)
const lerp = (a, b, t) => a + (b - a) * t

function hash(n) {
  const x = Math.sin(n * 127.1 + 311.7) * 43758.5453123
  return x - Math.floor(x)
}

function valueNoise(x) {
  const i = Math.floor(x)
  const f = x - i
  const u = f * f * (3 - 2 * f)
  return lerp(hash(i), hash(i + 1), u)
}

export function terrainHeight(x) {
  let h = 0
  h += 62 * Math.sin(x * 0.0009)
  h += 36 * Math.sin(x * 0.0021 + 1.7)
  h += 20 * Math.sin(x * 0.0043 + 2.3)
  h += 11 * (valueNoise(x * 0.005) - 0.5) * 2
  h += 4 * (valueNoise(x * 0.012 + 5.0) - 0.5) * 2
  return h
}

export const groundY = (x) => BASE_Y + terrainHeight(x)

export function createTerrain({ Bodies, Composite, world, CAT }) {
  const CHUNK_W = 288
  const SEG = 24
  const chunks = new Map()
  const ground = Composite.create({ label: 'ground' })
  Composite.add(world, ground)

  function buildChunk(ci) {
    const bodies = []
    const x0 = ci * CHUNK_W
    const n = Math.ceil(CHUNK_W / SEG)
    for (let i = 0; i < n; i++) {
      const ax = x0 + i * SEG
      const bx = ax + SEG
      const ay = groundY(ax)
      const by = groundY(bx)
      const len = Math.hypot(bx - ax, by - ay)
      const ang = Math.atan2(by - ay, bx - ax)
      const nx = -Math.sin(ang)
      const ny = Math.cos(ang)
      const thick = 80
      const cx = (ax + bx) / 2 + nx * (thick / 2)
      const cy = (ay + by) / 2 + ny * (thick / 2)
      bodies.push(
        Bodies.rectangle(cx, cy, len + 3, thick, {
          isStatic: true,
          angle: ang,
          friction: 1.1,
          frictionStatic: 2,
          restitution: 0.04,
          label: 'terrain',
          collisionFilter: {
            category: CAT.ground,
            mask: CAT.ground | CAT.hull | CAT.wheel | CAT.shell,
          },
        }),
      )
    }
    Composite.add(ground, bodies)
    chunks.set(ci, bodies)
  }

  function update(camX) {
    const lo = Math.floor((camX - 1600) / CHUNK_W)
    const hi = Math.floor((camX + 1600) / CHUNK_W)
    for (let c = lo; c <= hi; c++) if (!chunks.has(c)) buildChunk(c)
    for (const [c, bodies] of chunks) {
      if (c < lo - 2 || c > hi + 2) {
        Composite.remove(ground, bodies, true)
        chunks.delete(c)
      }
    }
  }

  return { update, groundY, BASE_Y, clamp, lerp }
}
