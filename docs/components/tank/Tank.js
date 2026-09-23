import { CONFIG, CATEGORY, HULL_POINTS, WHEEL_MOUNTS, IDLER_MOUNTS, TRACK as TRACK_CONFIG } from './config.js'
import { clamp } from './util.js'
import { Track } from './Track.js'

// The tank: one rigid hull held up by a set of sprung, driven wheel bodies.
//
// Where the old build computed spring/damper forces by hand and hard-clamped
// the body out of the ground, planck's WheelJoint now owns the suspension:
//   * each wheel is a real circle body that collides with the terrain (so the
//     bump stop is physical, not a positional hack);
//   * a WheelJoint springs and damps the wheel along its vertical axis;
//   * traction is still applied as a force at each grounded wheel, preserving
//     the original weight-transfer tuning.
export class Tank {
  constructor({ planck, world, terrain, input }) {
    this.planck = planck
    this.world = world
    this.terrain = terrain
    this.input = input

    this.hull = null
    this.wheels = []
    this.alive = false

    this.track = new Track({ planck, trackConfig: TRACK_CONFIG })

    // Scratch transform reused by view(); the interpolated values are copied out
    // before the next body overwrites it, so no per-frame allocation is needed.
    this._xf = planck.Transform.identity()
  }

  // --- lifecycle ------------------------------------------------------------

  // Spawn (or respawn) the tank at a world x, resting on its springs.
  spawn(x) {
    this.reset()
    const { Vec2, Polygon, WheelJoint } = this.planck
    const mount = WHEEL_MOUNTS[0]

    // Sit so every wheel rests at its natural length:
    // mount.y + restLength + radius = ground height.
    const hullY = this.terrain.heightAt(x) - (mount.y + CONFIG.suspensionRestLength + mount.radius)

    this.hull = this.world.createDynamicBody({
      position: Vec2(x, hullY),
      linearDamping: CONFIG.airResistance,
      angularDamping: CONFIG.angularResistance,
      allowSleep: false,
    })
    this.hull.createFixture(new Polygon(HULL_POINTS), {
      density: CONFIG.bodyDensity,
      friction: 0,
      // The hull never collides with anything; ground contact is handled by the
      // wheel bodies, which lets it drive smoothly over a heightfield.
      filterMaskBits: 0,
    })
    if (CONFIG.bodyRotationInertiaScale !== 1) {
      const mass = { mass: 0, center: Vec2(0, 0), I: 0 }
      this.hull.getMassData(mass)
      mass.I *= CONFIG.bodyRotationInertiaScale
      this.hull.setMassData(mass)
    }

    this.wheels = WHEEL_MOUNTS.map((m) => this._createWheel(m, x, hullY, WheelJoint))
    this.alive = true
  }

  _createWheel(mount, hullX, hullY, WheelJoint) {
    const { Circle, Vec2 } = this.planck
    // Neutral position: directly below the mount at rest length.
    const anchor = Vec2(hullX + mount.x, hullY + mount.y + CONFIG.suspensionRestLength)

    const body = this.world.createDynamicBody({ position: anchor, allowSleep: false })
    body.createFixture(new Circle(mount.radius), {
      density: CONFIG.wheelDensity,
      friction: CONFIG.wheelFriction,
      filterCategoryBits: CATEGORY.wheel,
      filterMaskBits: CATEGORY.ground,
    })

    // The joint's vertical axis is body A's local +y. A zero-length spring holds
    // the wheel at the anchor, so the anchor is the neutral wheel centre.
    const joint = this.world.createJoint(new WheelJoint({
      frequencyHz: CONFIG.suspensionFrequencyHz,
      dampingRatio: CONFIG.suspensionDampingRatio,
    }, this.hull, body, anchor, Vec2(0, 1)))

    return { mount, body, joint, radius: mount.radius, normalForce: 0, contact: false }
  }

  reset() {
    // Destroying the hull also destroys its joints, but not the wheel bodies on
    // the far side of those joints, so destroy the wheels explicitly first.
    for (const wheel of this.wheels) this.world.destroyBody(wheel.body)
    if (this.hull) this.world.destroyBody(this.hull)
    this.hull = null
    this.wheels = []
    this.alive = false
    this.track.reset()
  }

  // --- simulation -----------------------------------------------------------

  step(dt) {
    if (!this.alive) return
    this._applyTraction()
    this.world.step(dt / 1000, 8, 3)
    this._sampleContacts()
  }

  // Forward speed of the hull in px per 60 Hz frame. planck reports px/second,
  // so every speed compared against CONFIG has to be converted.
  get speedX() {
    return this.hull.getLinearVelocity().x / 60
  }

  // Traction as a force at each grounded wheel.
  _applyTraction() {
    const { Vec2 } = this.planck
    const grounded = this.wheels.filter((w) => w.contact)
    const vx = this.speedX
    const weight = this.hull.getMass() * CONFIG.gravity
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

    if (!grounded.length) return

    const perWheel = total / grounded.length
    let applied = 0
    for (const wheel of grounded) {
      const limit = gripLimit * wheel.normalForce
      const force = clamp(perWheel, -limit, limit)
      applied += force
      this.hull.applyForce(Vec2(force, 0), wheel.body.getPosition())
    }

    // Weight-transfer moment: traction acts below the centre of mass, so net
    // forward drive lifts the nose and braking dives it.
    if (applied !== 0) {
      const lever = applied > 0 ? CONFIG.accelerationPitchLever : CONFIG.brakingPitchLever
      this.hull.applyTorque(-applied * lever)
    }
  }

  // Read each wheel's ground normal force (used to cap traction and drive the
  // HUD compression gauge).
  _sampleContacts() {
    const invDt = 60
    const travel = CONFIG.suspensionTravel
    for (const wheel of this.wheels) {
      const edge = wheel.body.getContactList()
      wheel.contact = edge != null && edge.contact.isTouching()
      if (wheel.contact) {
        const reaction = wheel.joint.getReactionForce(invDt)
        wheel.normalForce = Math.abs(reaction.y)
      } else {
        wheel.normalForce = 0
      }
      // Translation is 0 at rest and negative as the wheel rises toward the
      // hull, so compression is the negative translation over the travel span.
      wheel.compression = clamp(-wheel.joint.getJointTranslation() / travel, 0, 1)
    }
  }

  // Scroll the track for the given belt loop and return the new phase. Called
  // once per rendered frame with wall-clock dt, so scroll speed is
  // display-refresh independent.
  scrollTrack(belt, dt) {
    if (this.hull) this.track.scroll(belt, this.hull.getLinearVelocity().x, dt)
    return this.track.phase
  }

  // --- geometry for the renderer --------------------------------------------

  // The body's interpolated transform between the previous and current physics
  // step. planck's Sweep.getTransform does this natively (it already blends
  // c0/a0 -> c/a by beta), so there is no need to hand-roll the lerp.
  _sweepTransform(body, t, out) {
    body.m_sweep.getTransform(out, t)
    return out
  }

  // Interpolated snapshot for rendering. `alpha` is how far we are between the
  // previous and current physics step (0..1).
  view(alpha) {
    if (!this.alive || !this.hull) return null
    const t = clamp(alpha, 0, 1)
    const { Transform, Vec2 } = this.planck
    const xf = this._xf

    this._sweepTransform(this.hull, t, xf)
    const pose = { x: xf.p.x, y: xf.p.y, angle: xf.q.getAngle() }

    // Built once and shared: the belt wraps these discs and the renderer draws them.
    const idlers = IDLER_MOUNTS.map((i) => {
      const p = Transform.mul(xf, Vec2(i.x, i.y))
      return { x: p.x, y: p.y, r: i.radius }
    })
    const wheels = this.wheels.map((wheel) => {
      this._sweepTransform(wheel.body, t, xf)
      return {
        x: xf.p.x,
        y: xf.p.y,
        radius: wheel.radius,
        // Interpolated spin sampled straight from the body's sweep transform.
        spinAngle: xf.q.getAngle(),
        compression: wheel.compression,
        contact: wheel.contact,
      }
    })
    const wheelDiscs = wheels.map((w) => ({ x: w.x, y: w.y, r: w.radius }))

    return {
      pose,
      wheels,
      idlers,
      belt: this.track.buildLoop(idlers.concat(wheelDiscs)),
      trackPhase: this.track.phase,
    }
  }

  // The muzzle position and direction for firing.
  muzzle() {
    if (!this.alive || !this.hull) return null
    const { Vec2 } = this.planck
    const p = this.hull.getWorldPoint(Vec2(120, -18))
    return { dir: this.hull.getAngle(), x: p.x, y: p.y }
  }
}
