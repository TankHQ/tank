import { CATEGORY } from './config.js'
import { valueNoise } from './util.js'

// The average ground height. The height function oscillates around this.
const BASE_Y = 360

// Collision chain window, in world pixels: it spans two camera-widths and is
// rebuilt only once the camera nears its edge.
const WINDOW = 6400
const MARGIN = 1600
const VERTEX_STEP = 24

// Procedural, effectively infinite terrain.
//
// The height function is shared by physics and rendering, so the drawn surface
// is exactly what the wheels roll on. Collision is a single planck Chain that
// slides along with the camera -- no per-chunk bodies to stream and release.
export class Terrain {
  constructor({ planck, world }) {
    this._planck = planck
    this._world = world

    // Pure function of x; no state.
    this.heightAt = (x) => {
      let h = 0
      h += 62 * Math.sin(x * 0.0009)
      h += 36 * Math.sin(x * 0.0021 + 1.7)
      h += 20 * Math.sin(x * 0.0043 + 2.3)
      h += 11 * (valueNoise(x * 0.005) - 0.5) * 2
      h += 4 * (valueNoise(x * 0.012 + 5.0) - 0.5) * 2
      return BASE_Y + h
    }

    this.body = null
    this._originX = Infinity
  }

  // Make sure a chain of height samples covers the camera, rebuilding it only
  // when the camera approaches the window edge.
  update(cameraX) {
    if (this.body && cameraX > this._originX + MARGIN && cameraX < this._originX + WINDOW - MARGIN) return

    const { Chain, Vec2 } = this._planck
    const originX = cameraX - WINDOW / 2
    const vertices = []
    for (let x = originX; x <= originX + WINDOW; x += VERTEX_STEP) {
      vertices.push(Vec2(x, this.heightAt(x)))
    }

    if (this.body) this._world.destroyBody(this.body)
    this.body = this._world.createBody()
    this.body.createFixture(new Chain(vertices, false), {
      friction: 1.1,
      filterCategoryBits: CATEGORY.ground,
      filterMaskBits: CATEGORY.wheel | CATEGORY.shell,
    })
    this._originX = originX
  }
}
