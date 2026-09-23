import { CONFIG, HULL_POINTS, WHEEL_MOUNTS, IDLER_MOUNTS, TRACK as TRACK_CONFIG } from './config.js'
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

    this.config = CONFIG
    this.hull = null
    this.wheels = []
    this.alive = false

    // Weight force per unit mass, taken from Matter's own gravity settings.
    this.gravityForcePerMass = engine.gravity.y * engine.gravity.scale

    this.track = new Track({ Matter, trackConfig: TRACK_CONFIG, idlerMounts: IDLER_MOUNTS })
  }

  // --- lifecycle ------------------------------------------------------------

  // Spawn (or respawn) the tank at a world x, resting on its springs.
  spawn(x) {
    this.reset()
    const { Bodies, Body, Composite } = this.Matter
    const config = this.config

    // Sit so every wheel rests at its natural length:
    // mount.y + restLength + radius = ground height.
    const mount = WHEEL_MOUNTS[0]
    const hullY = this.terrain.heightAt(x) - (mount.y + config.suspensionRestLength + mount.radius)

    this.hull = Bodies.fromVertices(x, hullY, [HULL_POINTS], {
      density: config.bodyDensity,
      frictionAir: config.airResistance,
      label: 'hull',
      // The hull never collides with anything; ground contact is handled by the
      // suspension, which is why it can drive smoothly over a heightfield.
      collisionFilter: { category: 0x0002, mask: 0 },
    })
    if (config.bodyRotationInertiaScale !== 1) {
      Body.setInertia(this.hull, this.hull.inertia * config.bodyRotationInertiaScale)
    }
    Composite.add(this.world, this.hull)

    this.wheels = WHEEL_MOUNTS.map((m) => new SuspensionWheel(config, m))
    this.alive = true
    this.track.reset()
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

  pose() {
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
    const pose = this.pose()
    const config = this.config
    const groundY = (x) => this.terrain.heightAt(x)
    const maxExtensionThisStep = config.wheelExtensionRate * (dt / (1000 / 60))
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
    const pose = this.pose()
    const { Body } = this.Matter
    const min = this.config.suspensionFullyCompressedLength

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
    const pose = this.pose()
    const groundY = (x) => this.terrain.heightAt(x)
    for (const wheel of this.wheels) wheel.settle(pose, groundY)
  }

  _applyTraction() {
    const config = this.config
    const { Body } = this.Matter
    const grounded = this.wheels.filter((w) => w.contact && w.normalForce > 0)
    if (!grounded.length) return

    const vx = this.hull.velocity.x
    const weight = this.hull.mass * this.gravityForcePerMass
    const { w, s } = this.input

    let total = 0
    let gripLimit = 0
    if (w && !s) {
      total = config.accelerationForce * weight * clamp(1 - vx / config.maximumSpeed, 0, 1)
      gripLimit = config.driveGripLimit
    } else if (s && !w) {
      if (vx > 0.4) {
        total = -config.brakingForce * weight * clamp(vx / 5, 0, 1)
        gripLimit = config.brakeGripLimit
      } else {
        total = -config.reverseForce * weight
        gripLimit = config.driveGripLimit
      }
    } else {
      total = -vx * config.coastingDrag * weight
      gripLimit = config.rollingResistanceGripLimit
    }

    const perWheel = total / grounded.length
    let applied = 0
    for (const wheel of grounded) {
      const limit = gripLimit * wheel.normalForce
      const force = clamp(perWheel, -limit, limit)
      applied += force
      Body.applyForce(this.hull, { x: wheel.contactX, y: wheel.contactY }, { x: force, y: 0 })
    }

    // Weight-transfer moment: traction acts below the centre of mass, so net
    // forward drive lifts the nose and braking dives it.
    if (applied !== 0) {
      const lever = applied > 0 ? config.accelerationPitchLever : config.brakingPitchLever
      this.hull.torque += -applied * lever
    }
  }

  _spinWheels(dt) {
    const groundSpeed = this.hull.velocity.x
    for (const wheel of this.wheels) wheel.spin(dt, groundSpeed)
  }

  // Scroll the track. Called once per rendered frame with wall-clock dt, so the
  // scroll speed does not depend on the display refresh rate.
  updateTrack(dt) {
    const loop = this.currentTrackLoop()
    this.track.scroll(loop, this.hull ? this.hull.velocity.x : 0, dt)
  }

  // --- geometry for the renderer --------------------------------------------

  _wheelCentres(pose, lengths) {
    return this.wheels.map((wheel, i) => {
      const centre = wheel.centrePoint(pose, lengths ? lengths[i] : wheel.currentLength)
      return { x: centre.x, y: centre.y, radius: wheel.radius }
    })
  }

  _idlerDiscs(pose) {
    return IDLER_MOUNTS.map((i) => {
      const p = toWorld(pose.x, pose.y, pose.angle, i.x, i.y)
      return { x: p.x, y: p.y, r: i.radius }
    })
  }

  currentTrackLoop() {
    if (!this.alive || !this.hull) return null
    return this.track.buildLoop(this.pose(), this._wheelCentres(this.pose()))
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
    const centres = this._wheelCentres(pose, lengths)
    return {
      pose,
      wheels: this.wheels.map((wheel, i) => wheel.view(pose, t)),
      idlers: this._idlerDiscs(pose),
      belt: this.track.buildLoop(pose, this._wheelCentres(pose, lengths)),
      trackPhase: this.track.phase,
    }
  }

  // The muzzle position and direction for firing.
  muzzle() {
    if (!this.alive || !this.hull) return null
    const dir = this.hull.angle
    const offset = { x: 120, y: -18 }
    const p = toWorld(this.hull.position.x, this.hull.position.y, dir, offset.x, offset.y)
    return { dir, x: p.x, y: p.y }
  }
}
