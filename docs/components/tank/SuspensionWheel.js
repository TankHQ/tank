import { clamp, mix, toWorld } from './util.js'

// Matter integrates velocity as v += (F/m) * dt^2. A damper proportional to
// instantaneous velocity is only stable while c*dt^2/m < 2, which is too weak
// to feel heavy. Clamping the damper to what one step can absorb keeps it
// stable at any strength.
const DT2 = (1000 / 60) * (1000 / 60)

// One independently sprung road wheel.
//
// The wheel is *virtual*: it is not a rigid body. It owns the suspension state
// (how far it is extended, whether it is touching the ground, how much load it
// carries) and knows how to compute its own spring/damper force. The Tank is
// responsible for supplying the ground height and the hull's motion, and for
// actually applying the force to the hull.
export class SuspensionWheel {
  constructor(config, mount) {
    this.config = config
    this.mountLocal = { x: mount.x, y: mount.y }
    this.radius = mount.radius

    // Suspension state.
    this.currentLength = config.suspensionRestLength
    this.compression = 0
    this.contact = false
    this.contactX = 0
    this.contactY = 0
    this.normalForce = 0
    this.spinAngle = 0

    // Previous-step values, for render interpolation.
    this.previousLength = this.currentLength
    this.previousSpin = 0
  }

  // --- geometry -------------------------------------------------------------

  // World-space position of the suspension mount (rotates with the body).
  mountPoint(pose) {
    return toWorld(pose.x, pose.y, pose.angle, this.mountLocal.x, this.mountLocal.y)
  }

  // World-space position of the wheel centre, given the current extension.
  centrePoint(pose, extension = this.currentLength) {
    return toWorld(pose.x, pose.y, pose.angle, this.mountLocal.x, this.mountLocal.y + extension)
  }

  // The extension at which this wheel would just touch the terrain, measured
  // straight down in world space. The terrain is a heightfield, so this is an
  // exact solve rather than a ray cast.
  groundContactLength(pose, groundY) {
    const mount = this.mountPoint(pose)
    return groundY(mount.x) - mount.y - this.radius
  }

  // --- suspension -----------------------------------------------------------

  // Snapshot the interpolatable fields before a physics step.
  capturePrevious() {
    this.previousLength = this.currentLength
    this.previousSpin = this.spinAngle
  }

  // Move the wheel toward the ground and record the resulting contact.
  //
  // Compression is instantaneous (the ground can shove the wheel up at once);
  // extension is rate-limited, which is the "rebound lag" that lets the tank
  // leave the ground over a sudden drop.
  advance(pose, groundY, maxExtensionThisStep) {
    const config = this.config
    const target = this.groundContactLength(pose, groundY)
    const min = config.suspensionFullyCompressedLength
    const max = config.suspensionFullyExtendedLength

    let contact = false
    if (target < min) {
      // driven past the bump stop: hold at the hard stop under full load
      this.currentLength = min
      contact = true
    } else if (target > max) {
      // ground out of reach: extend toward it, rate limited
      this.currentLength = Math.min(max, this.currentLength + maxExtensionThisStep)
    } else if (target <= this.currentLength) {
      // compressing: the ground has risen to meet the wheel
      this.currentLength = target
      contact = true
    } else {
      // extending: lag behind the terrain
      this.currentLength = Math.min(target, this.currentLength + maxExtensionThisStep)
      contact = this.currentLength >= target - 0.001
    }

    this._recordContact(pose, contact)
  }

  // After the hull has been clamped out of the ground, let any wheel the ground
  // has risen into compress instantly, so it never renders below the terrain.
  settle(pose, groundY) {
    const target = this.groundContactLength(pose, groundY)
    const min = this.config.suspensionFullyCompressedLength
    if (target < this.currentLength) {
      this.currentLength = Math.max(min, target)
      this._recordContact(pose, true)
    } else if (!this.contact) {
      this._recordContact(pose, false)
    }
  }

  _recordContact(pose, contact) {
    const max = this.config.suspensionRestLength - this.config.suspensionFullyCompressedLength
    this.compression = clamp((this.config.suspensionRestLength - this.currentLength) / max, 0, 1)
    this.contact = contact
    const mount = this.mountPoint(pose)
    this.contactX = mount.x
    this.contactY = mount.y + this.currentLength
    this.normalForce = 0
  }

  // Spring/damper force in weight units. `staticLoad` is the weight resting on
  // one wheel, `vDown` is the hull's downward velocity at the contact. The
  // spring carries the weight; the damper only ever removes velocity, so it can
  // never lift the chassis on its own.
  //
  // The damper is capped at the force that can remove one wheel's share of the
  // hull's downward velocity in a single step (its effective mass is the hull
  // mass split across the wheels), which keeps the explicit integrator stable
  // at any damping strength.
  suspensionForce(staticLoad, vDown, massPerWheel) {
    const config = this.config
    const travel = config.suspensionRestLength - config.suspensionFullyCompressedLength
    const u = clamp((config.suspensionRestLength - this.currentLength) / travel, 0, 1.6)
    const spring = staticLoad * (config.springLinearStiffness * u + config.springProgressiveStiffness * u * u * u)
    const wanted = config.shockAbsorberDamping * vDown
    const maxDamper = (massPerWheel * Math.abs(vDown)) / DT2
    const damper = clamp(wanted, -maxDamper, maxDamper)
    return Math.max(0, spring + damper)
  }

  // Advance the visual spin to match ground speed.
  spin(dt, groundSpeed) {
    this.spinAngle += (groundSpeed / this.radius) * (dt / 16.666)
  }

  // --- rendering ------------------------------------------------------------

  // Interpolated render record between the previous and current step.
  view(pose, alpha) {
    const length = mix(this.previousLength, this.currentLength, alpha)
    const spin = mix(this.previousSpin, this.spinAngle, alpha)
    const centre = this.centrePoint(pose, length)
    return {
      x: centre.x,
      y: centre.y,
      radius: this.radius,
      spinAngle: spin,
      compression: this.compression,
      contact: this.contact,
    }
  }
}
