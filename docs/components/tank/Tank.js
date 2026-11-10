import { CONFIG } from './config.js'
import { HULL_POINTS, WHEEL_MOUNTS, IDLER_MOUNTS, TRACK as TRACK_CONFIG, GUN, SUSPENSION } from './geometry.js'
import { clamp, mix, mixAngle, toWorld } from './util.js'
import { bodyMatrix, bodyDirection, apply } from './transform.js'
import { SuspensionWheel } from './SuspensionWheel.js'
import { Track } from './Track.js'

export class Tank {
    constructor({ Matter, engine, world, terrain, input }) {
        this.Matter = Matter
        this.engine = engine
        this.world = world
        this.terrain = terrain
        this.input = input

        this.hull = null
        this.wheels = []
        this.alive = false
        this._idlerSpin = 0 // Cumulative idler rotation (rad)

        this.gravityForcePerMass = engine.gravity.y * engine.gravity.scale
        this.track = new Track({ Matter, trackConfig: TRACK_CONFIG })
    }

    // Spawn the tank at world X (px), resting at unloaded spring length.
    spawn(x) {
        this.reset()
        const { Bodies, Body, Composite } = this.Matter

        const mount = WHEEL_MOUNTS[0]
        const hullY = this.terrain.heightAt(x) - (mount.y + SUSPENSION.restLength + mount.radius)

        this.hull = Bodies.fromVertices(x, hullY, [HULL_POINTS], {
            density: CONFIG.bodyDensity,
            frictionAir: CONFIG.airResistance,
            label: 'hull',
            collisionFilter: { mask: 0 }, // Ground contact is resolved solely via suspension rays
        })
        if (CONFIG.bodyRotationInertiaScale !== 1) {
            Body.setInertia(this.hull, this.hull.inertia * CONFIG.bodyRotationInertiaScale)
        }
        Composite.add(this.world, this.hull)

        this.wheels = WHEEL_MOUNTS.map((m) => new SuspensionWheel(m))
        this.alive = true
        this._capturePrevious()
    }

    reset() {
        if (this.hull) this.Matter.Composite.remove(this.world, this.hull, true)
        this.hull = null
        this.wheels = []
        this.alive = false
        this.track.reset()
    }

    step(dt) {
        if (!this.alive) return
        this._capturePrevious()
        this._applySuspension(dt)
        this._applyTraction()
        this.Matter.Engine.update(this.engine, dt)
        this._enforceGround()
        this._settleWheels()
        this._spinWheels(dt)
    }

    _pose() {
        return { x: this.hull.position.x, y: this.hull.position.y, angle: this.hull.angle }
    }

    _capturePrevious() {
        if (!this.hull) return
        for (const w of this.wheels) w.capturePrevious()
    }

    _staticLoad() {
        return (this.hull.mass * this.gravityForcePerMass) / this.wheels.length
    }

    _massPerWheel() {
        return this.hull.mass / this.wheels.length
    }

    _applySuspension(dt) {
        const pose = this._pose()
        const groundY = (x) => this.terrain.heightAt(x)
        const maxExtensionThisStep = CONFIG.wheelExtensionRate * (dt / (1000 / 60))
        const staticLoad = this._staticLoad()
        const massPerWheel = this._massPerWheel()
        const { Body } = this.Matter

        for (const wheel of this.wheels) {
            wheel.advance(pose, groundY, maxExtensionThisStep)
            if (!wheel.contact) continue

            const mount = wheel.mountPoint(pose)
            const vDown = this.hull.velocity.y + this.hull.angularVelocity * (mount.x - this.hull.position.x)
            const force = wheel.suspensionForce(staticLoad, vDown, massPerWheel)
            wheel.normalForce = force
            Body.applyForce(this.hull, { x: wheel.contactX, y: wheel.contactY }, { x: 0, y: -force })
        }
    }

    // Clamp hull upward (px) so no wheel compresses past fullyCompressedLength.
    _enforceGround() {
        const pose = this._pose()
        const { Body } = this.Matter
        const min = SUSPENSION.fullyCompressedLength

        let push = 0
        for (const wheel of this.wheels) {
            const mount = wheel.mountPoint(pose)
            const penetration = mount.y + min + wheel.radius - this.terrain.heightAt(mount.x)
            if (penetration > push) push = penetration
        }
        if (push > 0) {
            Body.setPosition(this.hull, { x: this.hull.position.x, y: this.hull.position.y - push })
            if (this.hull.velocity.y > 0) {
                Body.setVelocity(this.hull, { x: this.hull.velocity.x, y: 0 })
            }
        }
    }

    _settleWheels() {
        const pose = this._pose()
        const groundY = (x) => this.terrain.heightAt(x)
        for (const wheel of this.wheels) wheel.settle(pose, groundY)
    }

    _applyTraction() {
        const { Body } = this.Matter
        const grounded = this.wheels.filter((w) => w.contact && w.normalForce > 0)
        if (!grounded.length) return

        const vx = this.hull.velocity.x
        const weight = this.hull.mass * this.gravityForcePerMass
        const { w, s, cruise } = this.input

        let total = 0
        let gripLimit = 0
        if (cruise) {
            if (vx < CONFIG.slowSpeed) {
                total = CONFIG.accelerationForce * weight * clamp(1 - vx / CONFIG.slowSpeed, 0, 1)
                gripLimit = CONFIG.driveGripLimit
            } else {
                total = -vx * CONFIG.coastingDrag * 2 * weight
                gripLimit = CONFIG.rollingResistanceGripLimit
            }
        } else if (w && !s) {
            total = CONFIG.accelerationForce * weight * clamp(1 - vx / CONFIG.maximumSpeed, 0, 1)
            gripLimit = CONFIG.driveGripLimit
        } else if (s && !w) {
            if (vx > 0.4) {
                total = -CONFIG.brakingForce * weight * clamp(vx / 5, 0, 1)
                gripLimit = CONFIG.brakeGripLimit
            } else {
                total = -CONFIG.reverseForce * weight
                gripLimit = CONFIG.driveGripLimit
            }
        } else {
            total = -vx * CONFIG.coastingDrag * weight
            gripLimit = CONFIG.rollingResistanceGripLimit
        }

        // Apply horizontal traction at tractionArm (px) below wheel contact to induce pitch torque.
        const arm = CONFIG.tractionArm
        const perWheel = total / grounded.length
        for (const wheel of grounded) {
            const limit = gripLimit * wheel.normalForce
            const force = clamp(perWheel, -limit, limit)
            Body.applyForce(this.hull, { x: wheel.contactX, y: wheel.contactY + arm }, { x: force, y: 0 })
        }
    }

    _spinWheels(dt) {
        const groundSpeed = this.hull.velocity.x
        for (const wheel of this.wheels) wheel.spin(dt, groundSpeed)
        const idlerRadius = IDLER_MOUNTS[0].radius
        this._idlerSpin += (groundSpeed / idlerRadius) * (dt / 16.666)
    }

    scrollTrack(belt, dt) {
        if (this.hull) this.track.scroll(belt, this.hull.velocity.x, dt)
        return this.track.phase
    }

    _idlerDiscs(pose) {
        return IDLER_MOUNTS.map((i) => {
            const p = toWorld(pose.x, pose.y, pose.angle, i.x, i.y)
            return { x: p.x, y: p.y, r: i.radius, spin: this._idlerSpin }
        })
    }

    // Interpolated render snapshot for alpha in 0..1.
    view(alpha) {
        if (!this.alive || !this.hull) return null
        const t = clamp(alpha, 0, 1)
        const pose = {
            x: mix(this.hull.positionPrev.x, this.hull.position.x, t),
            y: mix(this.hull.positionPrev.y, this.hull.position.y, t),
            angle: mixAngle(this.hull.anglePrev, this.hull.angle, t),
        }
        const lengths = this.wheels.map((wheel) => mix(wheel.previousLength, wheel.currentLength, t))

        const idlers = this._idlerDiscs(pose)
        const wheelDiscs = this.wheels.map((wheel, i) => {
            const centre = wheel.centrePoint(pose, lengths[i])
            return { x: centre.x, y: centre.y, r: wheel.radius }
        })

        return {
            pose,
            wheels: this.wheels.map((wheel) => wheel.view(pose, t)),
            idlers,
            belt: this.track.buildLoop(idlers.concat(wheelDiscs), (x) => this.terrain.heightAt(x)),
            trackPhase: this.track.phase,
        }
    }

    // World-space muzzle position (px) and firing angle (rad).
    muzzle() {
        if (!this.alive || !this.hull) return null
        const pose = {
            x: this.hull.position.x,
            y: this.hull.position.y,
            angle: this.hull.angle,
        }
        const p = apply(bodyMatrix(pose), GUN.muzzleX, GUN.muzzleY)
        const dir = bodyDirection(pose)
        return { dir, x: p.x, y: p.y }
    }
}
