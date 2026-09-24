import { CONFIG, BODY } from './config.js'
import { Terrain } from './Terrain.js'
import { Tank } from './Tank.js'
import { Camera } from './Camera.js'
import { ParticleField } from './ParticleField.js'
import { ShellManager } from './ShellManager.js'
import { Renderer } from './Renderer.js'

// Collision categories. The hull and wheels are deliberately *not* physical
// bodies (the suspension handles ground contact), so only terrain and shells
// have real collision filters.
const COLLISION_CATEGORIES = {
  ground: 0x0001,
  shell: 0x0008,
  groundAndShell: 0x0001 | 0x0008,
}

const FIXED_STEP_MS = 1000 / 60
const MAX_STEPS_PER_FRAME = 5

// Owns the Matter engine, all the scene objects, and the fixed-timestep game
// loop. The Vue component just creates one of these and drives it.
export class Game {
  constructor({ Matter, canvas, base }) {
    this.Matter = Matter
    this.running = false
    this._raf = 0
    this._lastTime = 0
    this._accumulator = 0

    this._dist = 0
    this._airtime = 0
    this._fpsAccumulator = 0
    this._fpsFrames = 0
    this.fps = 0
    this._hudAccumulator = 0

    const { Engine, Composite } = Matter
    this.engine = Engine.create()
    this.engine.gravity.y = CONFIG.gravity
    this.world = this.engine.world

    this.terrain = new Terrain({ Matter, world: this.world, collisionCategories: COLLISION_CATEGORIES })
    this.terrain.update(0)

    this.particles = new ParticleField()
    this.shells = new ShellManager({
      Matter,
      world: this.world,
      collisionCategories: COLLISION_CATEGORIES,
      particles: this.particles,
    })
    this.input = { w: false, s: false }
    this.tank = new Tank({ Matter, engine: this.engine, world: this.world, terrain: this.terrain, input: this.input })
    this.camera = new Camera()
    this.renderer = new Renderer(canvas, base)

    this._dustTimer = 0
    this._onHud = null
  }

  // Called once per HUD refresh (a few times a second) with a stats object.
  onHud(callback) {
    this._onHud = callback
  }

  start(rect) {
    this.renderer.resize(rect)
    this.tank.spawn(0)
    this.camera.reset()
    this.running = true
    this._lastTime = 0
    this._raf = requestAnimationFrame((t) => this._frame(t))
  }

  stop() {
    this.running = false
    cancelAnimationFrame(this._raf)
    this.Matter.Engine.clear(this.engine)
    this.Matter.Composite.clear(this.world, false)
  }

  resize(rect) {
    this.renderer.resize(rect)
  }

  reset() {
    this.tank.spawn(0)
    this._dist = 0
    this.particles.clear()
    this.shells.clear()
  }

  fire() {
    const shot = this.tank.muzzle()
    if (!shot) return
    this.shells.launch(shot)
    this.camera.addShake(11)
  }

  // --- loop -----------------------------------------------------------------

  _frame(now) {
    if (!this.running) return
    this._raf = requestAnimationFrame((t) => this._frame(t))
    if (!this._lastTime) this._lastTime = now
    let dt = now - this._lastTime
    this._lastTime = now
    if (dt > 100) dt = 100
    this._accumulator += dt

    // Fixed timestep: physics advances in whole 60 Hz steps no matter the
    // display refresh rate.
    let steps = 0
    while (this._accumulator >= FIXED_STEP_MS && steps < MAX_STEPS_PER_FRAME) {
      this._physicsStep(FIXED_STEP_MS)
      this._accumulator -= FIXED_STEP_MS
      steps++
    }
    if (steps === MAX_STEPS_PER_FRAME) this._accumulator = 0

    this._updateEffects(dt)

    // Render from an interpolated pose between the last two physics steps,
    // which is what removes jitter on high-refresh displays.
    const alpha = this._accumulator / FIXED_STEP_MS
    const view = this.tank.view(alpha)
    if (view) view.trackPhase = this.tank.scrollTrack(view.belt, dt)
    // Normalised speed biases the camera framing: forward pulls the tank left to
    // reveal the road ahead, reverse pushes it right.
    const hull = this.tank.hull
    const speed01 = hull ? hull.velocity.x / CONFIG.cameraCruiseSpeed : 0
    this.camera.follow(view ? view.pose : null, this.renderer.width, this.renderer.height, speed01)
    this.renderer.render({
      camera: this.camera,
      view,
      groundY: (x) => this.terrain.heightAt(x),
      particles: this.particles.particles,
      shells: this.shells.shells,
    })

    this._updateStats(dt)
  }

  _physicsStep(dt) {
    this.terrain.update(this.camera.x)
    this.tank.step(dt)
  }

  _updateEffects(dt) {
    if (this.tank.alive) {
      const hull = this.tank.hull
      this._dist = Math.max(this._dist, hull.position.x)
      const aboveGround = hull.position.y < this.terrain.heightAt(hull.position.x) - 130
      this._airtime = aboveGround ? this._airtime + dt / 1000 : 0

      if (Math.abs(hull.angle) > 2.1 || hull.position.y > this.terrain.heightAt(hull.position.x) + 400) {
        this.reset()
      }

      // Trailing exhaust while moving.
      if (Math.abs(hull.velocity.x) > 0.2) {
        this._dustTimer++
        if (this._dustTimer > 6) {
          this._dustTimer = 0
          const exhaust = { x: hull.position.x, y: hull.position.y }
          const cos = Math.cos(hull.angle)
          const sin = Math.sin(hull.angle)
          // Exhaust leaves the rear of the hull, so scale it with the body.
          const rear = BODY.length * 0.2
          this.particles.exhaust(
            exhaust.x + cos * -rear,
            exhaust.y + sin * -rear - BODY.depth * 0.42,
            hull.velocity.x,
          )
        }
      }
    }

    this.particles.update(dt)
    this.shells.update(dt)
  }

  _updateStats(dt) {
    this._fpsAccumulator += dt
    this._fpsFrames++
    if (this._fpsAccumulator > 500) {
      this.fps = Math.round(this._fpsFrames / (this._fpsAccumulator / 1000))
      this._fpsAccumulator = 0
      this._fpsFrames = 0
    }

    this._hudAccumulator += dt
    if (this._hudAccumulator > 60 && this._onHud) {
      this._hudAccumulator = 0
      const hull = this.tank.hull
      this._onHud({
        // px/frame -> m/s -> km/h, and px -> m, using the world scale.
        speed: hull
          ? Math.round(Math.abs(hull.velocity.x) * 60 / CONFIG.pixelsPerMetre * 3.6)
          : 0,
        distance: Math.round(this._dist / CONFIG.pixelsPerMetre),
        throttle: this.input.w ? 1 : this.input.s ? -1 : 0,
        airtime: this._airtime,
        fps: this.fps,
        wheels: this.tank.wheels.map((w) => w.compression),
      })
    }
  }
}

