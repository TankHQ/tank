import Matter from 'matter-js'
import { CONFIG, CAMERA } from './config.js'
import { BODY } from './geometry.js'
import { Terrain } from './Terrain.js'
import { Tank } from './Tank.js'
import { Camera } from './Camera.js'
import { ParticleField } from './ParticleField.js'
import { ShellManager } from './ShellManager.js'
import { Renderer } from './Renderer.js'

const COLLISION_CATEGORIES = {
    ground: 0x0001,
    shell: 0x0008,
    groundAndShell: 0x0001 | 0x0008,
}

const FIXED_STEP_MS = 1000 / 60 // Physics step duration (ms)
const MAX_STEPS_PER_FRAME = 5

export class Game {
    constructor({ canvas, base }) {
        this.Matter = Matter
        this.running = false
        this._raf = 0
        this._lastTime = 0
        this._accumulator = 0

        const { Engine } = Matter
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
        this.input = { w: false, s: false, cruise: true }
        this.tank = new Tank({ Matter, engine: this.engine, world: this.world, terrain: this.terrain, input: this.input })
        this.camera = new Camera()
        this.renderer = new Renderer(canvas, base)

        this._dustTimer = 0
    }

    start(rect) {
        this.renderer.resize(rect)
        this.tank.spawn(0)
        this.camera.reset(this.tank.hull?.position, this.renderer.width, this.renderer.height)
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
        this.particles.clear()
        this.shells.clear()
    }

    fire() {
        const shot = this.tank.muzzle()
        if (!shot) return
        this.shells.launch(shot)
        this.camera.addShake(11)
    }

    _frame(now) {
        if (!this.running) return
        this._raf = requestAnimationFrame((t) => this._frame(t))
        if (!this._lastTime) this._lastTime = now
        let dt = now - this._lastTime
        this._lastTime = now
        if (dt > 100) dt = 100
        this._accumulator += dt

        // Step physics in fixed 60 Hz increments (FIXED_STEP_MS).
        let steps = 0
        while (this._accumulator >= FIXED_STEP_MS && steps < MAX_STEPS_PER_FRAME) {
            this._physicsStep(FIXED_STEP_MS)
            this._accumulator -= FIXED_STEP_MS
            steps++
        }
        if (steps === MAX_STEPS_PER_FRAME) this._accumulator = 0

        this._updateEffects(dt)

        // Interpolate between previous and current physics poses by alpha (0..1).
        const alpha = this._accumulator / FIXED_STEP_MS
        const view = this.tank.view(alpha)
        if (view) view.trackPhase = this.tank.scrollTrack(view.belt, dt)
        const hull = this.tank.hull
        const speed01 = hull ? hull.velocity.x / CAMERA.cruiseSpeed : 0 // Normalized speed (-1..1)
        this.camera.follow(view ? view.pose : null, this.renderer.width, this.renderer.height, speed01, dt)
        this.renderer.render({
            camera: this.camera,
            view,
            groundY: (x) => this.terrain.heightAt(x),
            particles: this.particles.particles,
            shells: this.shells.shells,
        })
    }

    _physicsStep(dt) {
        this.terrain.update(this.camera.x)
        this.tank.step(dt)
    }

    _updateEffects(dt) {
        if (this.tank.alive) {
            const hull = this.tank.hull

            if (Math.abs(hull.angle) > 2.1 || hull.position.y > this.terrain.heightAt(hull.position.x) + 400) {
                this.reset()
            }

            if (Math.abs(hull.velocity.x) > 0.2) {
                this._dustTimer++
                if (this._dustTimer > 6) {
                    this._dustTimer = 0
                    const exhaust = { x: hull.position.x, y: hull.position.y }
                    const cos = Math.cos(hull.angle)
                    const sin = Math.sin(hull.angle)
                    const rear = BODY.length * 0.2 // Rear exhaust offset from hull center (px)
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
}
