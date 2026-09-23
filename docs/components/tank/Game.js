import { CONFIG } from './config.js'
import { Terrain } from './Terrain.js'
import { Tank } from './Tank.js'
import { Camera } from './Camera.js'
import { ParticleField } from './ParticleField.js'
import { ShellManager } from './ShellManager.js'
import { Renderer } from './Renderer.js'

const FIXED_STEP_MS = 1000 / 60
const MAX_STEPS_PER_FRAME = 5

// Owns the planck world, all the scene objects, and the fixed-timestep game
// loop. The Vue component just creates one of these and drives it.
export class Game {
  constructor({ planck, canvas, base }) {
    this.planck = planck
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

    const { World, Vec2, Settings } = planck
    // The world runs in pixels, but planck caps a body's movement to
    // `maxTranslation` units per step (default 2). At 60 Hz that silently caps
    // every body at 120 px/s, far below the tank's top speed, so raise it.
    Settings.maxTranslation = 60
    this.world = new World({ gravity: Vec2(0, CONFIG.gravity), allowSleep: false })

    this.terrain = new Terrain({ planck, world: this.world })
    this.terrain.update(0)

    this.particles = new ParticleField()
    this.shells = new ShellManager({ planck, world: this.world, particles: this.particles })
    this.input = { w: false, s: false }
    this.tank = new Tank({ planck, world: this.world, terrain: this.terrain, input: this.input })
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
    this.camera.follow(view ? view.pose : null, this.renderer.width, this.renderer.height)
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
      const hull = this.tank.hull.getPosition()
      this._dist = Math.max(this._dist, hull.x)
      const aboveGround = hull.y < this.terrain.heightAt(hull.x) - 130
      this._airtime = aboveGround ? this._airtime + dt / 1000 : 0

      if (Math.abs(this.tank.hull.getAngle()) > 2.1 || hull.y > this.terrain.heightAt(hull.x) + 400) {
        // Respawning moves the hull; skip the rest of this frame's hull-based
        // effects so the stale position is never used.
        this.reset()
      } else if (Math.abs(this.tank.speedX) > 0.2) {
        // Trailing exhaust while moving.
        this._dustTimer++
        if (this._dustTimer > 6) {
          this._dustTimer = 0
          const angle = this.tank.hull.getAngle()
          const cos = Math.cos(angle)
          const sin = Math.sin(angle)
          this.particles.exhaust(
            hull.x + cos * -112,
            hull.y + sin * -112 - 28,
            this.tank.speedX,
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
        speed: hull ? Math.round(Math.abs(this.tank.speedX) * 12) : 0,
        distance: Math.round(this._dist / 10),
        throttle: this.input.w ? 1 : this.input.s ? -1 : 0,
        airtime: this._airtime,
        fps: this.fps,
        wheels: this.tank.wheels.map((w) => w.compression),
      })
    }
  }
}
