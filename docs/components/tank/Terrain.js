import { valueNoise } from './util.js'

// The average ground height. The height function oscillates around this.
const BASE_Y = 360

// Procedural, effectively infinite terrain.
//
// The height function is shared by physics and rendering, so the drawn surface
// is exactly what the wheels probe against. Physics collision bodies are
// streamed in chunks around the camera and discarded behind it.
export class Terrain {
  constructor({ Matter, world, collisionCategories }) {
    this._Bodies = Matter.Bodies
    this._Composite = Matter.Composite
    this._categories = collisionCategories

    this._chunkWidth = 288
    this._segmentWidth = 24
    this._chunks = new Map()

    this._ground = this._Composite.create({ label: 'ground' })
    this._Composite.add(world, this._ground)
  }

  // Ground height at a world x. Pure function of x; no state.
  heightAt(x) {
    let h = 0
    h += 62 * Math.sin(x * 0.0009)
    h += 36 * Math.sin(x * 0.0021 + 1.7)
    h += 20 * Math.sin(x * 0.0043 + 2.3)
    h += 11 * (valueNoise(x * 0.005) - 0.5) * 2
    h += 4 * (valueNoise(x * 0.012 + 5.0) - 0.5) * 2
    return BASE_Y + h
  }

  _buildChunk(chunkIndex) {
    const bodies = []
    const originX = chunkIndex * this._chunkWidth
    const segments = Math.ceil(this._chunkWidth / this._segmentWidth)
    for (let i = 0; i < segments; i++) {
      const ax = originX + i * this._segmentWidth
      const bx = ax + this._segmentWidth
      const ay = this.heightAt(ax)
      const by = this.heightAt(bx)
      const length = Math.hypot(bx - ax, by - ay)
      const angle = Math.atan2(by - ay, bx - ax)
      const normalX = -Math.sin(angle)
      const normalY = Math.cos(angle)
      const thickness = 80
      bodies.push(
        this._Bodies.rectangle(
          (ax + bx) / 2 + normalX * (thickness / 2),
          (ay + by) / 2 + normalY * (thickness / 2),
          length + 3,
          thickness,
          {
            isStatic: true,
            angle,
            friction: 1.1,
            frictionStatic: 2,
            restitution: 0.04,
            label: 'terrain',
            collisionFilter: { category: this._categories.ground, mask: this._categories.groundAndShell },
          },
        ),
      )
    }
    this._Composite.add(this._ground, bodies)
    this._chunks.set(chunkIndex, bodies)
  }

  // Stream collision chunks in around the camera and release distant ones.
  update(cameraX) {
    const span = 1600
    const lo = Math.floor((cameraX - span) / this._chunkWidth)
    const hi = Math.floor((cameraX + span) / this._chunkWidth)
    for (let c = lo; c <= hi; c++) if (!this._chunks.has(c)) this._buildChunk(c)
    for (const [c, bodies] of this._chunks) {
      if (c < lo - 2 || c > hi + 2) {
        this._Composite.remove(this._ground, bodies, true)
        this._chunks.delete(c)
      }
    }
  }
}
