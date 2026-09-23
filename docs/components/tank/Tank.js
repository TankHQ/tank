import { CONFIG, HULL_POINTS, WHEEL_MOUNTS, IDLER_MOUNTS, TRACK as TRACK_CONFIG, GUN } from './config.js'
import { clamp, mix, mixAngle, toWorld } from './util.js'
import { SuspensionWheel } from './SuspensionWheel.js'
import { Track } from './Track.js'

// The tank: one rigid hull, a set of independently sprung virtual wheels, and
// a track belt.
//
// Each step:
//   1. Each wheel casts straight down, resolves its spring length and applies
//      its suspension force to the hull at the contact point.
//   2. Traction is applied at those contacts, capped by grip and by how much
//      weight each wheel carries. That weight transfer is what makes it squat
//      under power and dive under braking.
//   3. Matter integrates the hull.
//   4. A hard constraint guarantees no wheel sinks past its bump stop, so the
//      hull can never penetrate the terrain.
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
    this._idlerSpin = 0

    // Weight force per unit mass, taken from Matter's own gravity settings.
    this.gravityForcePerMass = engine.gravity.y * engine.gravity.scale

    this.track = new Track({ Matter, trackConfig: TRACK_CONFIG })
  }

  // --- lifecycle ------------------------------------------------------------

  // Spawn (or respawn) the tank at a world x, resting on its springs.
  spawn(x) {
    this.reset()
    const { Bodies, Body, Composite } = this.Matter

    // Sit so every wheel rests at its natural length. The wheel centres are at
    // WHEEL_MOUNTS[i].y + restLength, derived from the running gear.
    const mount = WHEEL_MOUNTS[0]
    const hullY = this.terrain.heightAt(x) - (mount.y + CONFIG.suspensionRestLength + mount.radius)

    this.hull = Bodies.fromVertices(x, hullY, [HULL_POINTS], {
      density: CONFIG.bodyDensity,
      frictionAir: CONFIG.airResistance,
      label: 'hull',
      // The hull never collides with anything; ground contact is handled by the
      // suspension, which is why it can drive smoothly over a heightfield. A
      // zero mask disables all collisions, so no category is needed.
      collisionFilter: { mask: 0 },
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

  // --- simulation -----------------------------------------------------------

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

  // Weight resting on one wheel. Drives the spring rate.
  _staticLoad() {
    return (this.hull.mass * this.gravityForcePerMass) / this.wheels.length
  }

  // Effective mass behind one wheel's share of the motion, used to cap the
  // damper so the explicit integrator stays stable.
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

      // The hull's downward velocity at the contact point (includes the body's
      // rotation), which the damper acts against.
      const mount = wheel.mountPoint(pose)
      const vDown = this.hull.velocity.y + this.hull.angularVelocity * (mount.x - this.hull.position.x)
      const force = wheel.suspensionForce(staticLoad, vDown, massPerWheel)
      wheel.normalForce = force
      Body.applyForce(this.hull, { x: wheel.contactX, y: wheel.contactY }, { x: 0, y: -force })
    }
  }

  // Guarantee no wheel penetrates past its bump stop. The spring can be
  // overpowered by a fast hit; this is a positional constraint, so it cannot.
  _enforceGround() {
    const pose = this._pose()
    const { Body } = this.Matter
    const min = CONFIG.suspensionFullyCompressedLength

    let push = 0
    for (const wheel of this.wheels) {
      const mount = wheel.mountPoint(pose)
      const penetration = mount.y + min + wheel.radius - this.terrain.heightAt(mount.x)
      if (penetration > push) push = penetration
    }
    if (push > 0) {
      // Body.setPosition keeps vertices/bounds consistent; Body.setVelocity is
      // required because Matter's Verlet integrator ignores a raw field write.
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
    const { w, s } = this.input

    let total = 0
    let gripLimit = 0
    if (w && !s) {
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

    // Traction is applied at the contact point, below the centre of mass, so
    // the nose lifting under power and the dive under braking fall out of the
    // suspension naturally: the hull pitches, the spring loads shift, and the
    // normal forces redistribute. No extra torque is needed.
    const perWheel = total / grounded.length
    for (const wheel of grounded) {
      const limit = gripLimit * wheel.normalForce
      const force = clamp(perWheel, -limit, limit)
      Body.applyForce(this.hull, { x: wheel.contactX, y: wheel.contactY }, { x: force, y: 0 })
    }
  }

  _spinWheels(dt) {
    const groundSpeed = this.hull.velocity.x
    for (const wheel of this.wheels) wheel.spin(dt, groundSpeed)
    // Idlers are fixed to the hull but roll against the same track, so their
    // visual spin follows ground speed over their own radius.
    const idlerRadius = IDLER_MOUNTS[0].radius
    this._idlerSpin += (groundSpeed / idlerRadius) * (dt / 16.666)
  }

  // Scroll the track for the given belt loop and return the new phase. Called
  // once per rendered frame with wall-clock dt, so scroll speed is
  // display-refresh independent.
  scrollTrack(belt, dt) {
    if (this.hull) this.track.scroll(belt, this.hull.velocity.x, dt)
    return this.track.phase
  }

  // --- geometry for the renderer --------------------------------------------

  // World-space idler discs (idlers are fixed to the hull, not sprung). They
  // spin with ground speed just like the road wheels.
  _idlerDiscs(pose) {
    return IDLER_MOUNTS.map((i) => {
      const p = toWorld(pose.x, pose.y, pose.angle, i.x, i.y)
      return { x: p.x, y: p.y, r: i.radius, spin: this._idlerSpin }
    })
  }

  // Interpolated snapshot for rendering. `alpha` is how far we are between the
  // previous and current physics step (0..1). Matter keeps the hull's previous
  // transform in `positionPrev`/`anglePrev`; wheels keep their own snapshots.
  view(alpha) {
    if (!this.alive || !this.hull) return null
    const t = clamp(alpha, 0, 1)
    const pose = {
      x: mix(this.hull.positionPrev.x, this.hull.position.x, t),
      y: mix(this.hull.positionPrev.y, this.hull.position.y, t),
      angle: mixAngle(this.hull.anglePrev, this.hull.angle, t),
    }
    const lengths = this.wheels.map((wheel) => mix(wheel.previousLength, wheel.currentLength, t))

    // Built once and shared: the belt wraps these discs and the renderer draws them.
    const idlers = this._idlerDiscs(pose)
    const wheelDiscs = this.wheels.map((wheel, i) => {
      const centre = wheel.centrePoint(pose, lengths[i])
      return { x: centre.x, y: centre.y, r: wheel.radius }
    })

    return {
      pose,
      wheels: this.wheels.map((wheel) => wheel.view(pose, t)),
      idlers,
      belt: this.track.buildLoop(idlers.concat(wheelDiscs)),
      trackPhase: this.track.phase,
    }
  }

  // The muzzle position and direction for firing.
  muzzle() {
    if (!this.alive || !this.hull) return null
    const dir = this.hull.angle
    // Tip of the drawn barrel, derived from the running gear / hull.
    const offset = { x: GUN.muzzleX, y: GUN.muzzleY }
    const p = toWorld(this.hull.position.x, this.hull.position.y, dir, offset.x, offset.y)
    return { dir, x: p.x, y: p.y }
  }
}
