import { CAMERA } from './config.js'
import { clamp, mix } from './util.js'

// Smoothly follows a target point. Uses exponential smoothing with a fixed
// per-step factor; it is fed the *interpolated* render position so the tank and
// the world stay locked together on high-refresh displays.
//
// The horizontal framing also reacts to speed. The tank's screen position is
// `viewportWidth * targetX`, so a smaller `targetX` places it further left:
//   * moving forward, `targetX` eases toward `forwardTargetX`, leaving most of
//     the screen for the road ahead and only a little space behind the tank;
//   * reversing, it eases toward `reverseTargetX`, showing the road behind;
//   * stopped, it eases back to `baseTargetX` (the centred start).
// The easing is deliberately slow, so the shift is progressive rather than
// snapping with the throttle. All the numbers live in `CAMERA` (config.js).
export class Camera {
  constructor(cfg = CAMERA) {
    this.cfg = cfg
    this.x = 0
    this.y = 0
    this.targetX = cfg.baseTargetX
    this.shake = 0
    this.shakeDecay = 0.88
  }

  // `speed01` is the tank's forward speed as a fraction of the cruise speed
  // (negative when reversing), used to bias the framing.
  follow(pose, viewportWidth, viewportHeight, speed01 = 0) {
    const { baseTargetX, forwardTargetX, reverseTargetX, targetY, framingSmoothing, smoothingX, smoothingY, idleSmoothing } = this.cfg
    const v = clamp(speed01, -1, 1)
    const desired = v >= 0
      ? mix(baseTargetX, forwardTargetX, v)
      : mix(baseTargetX, reverseTargetX, -v)
    this.targetX = mix(this.targetX, desired, framingSmoothing)

    if (pose) {
      this.x = mix(this.x, pose.x - viewportWidth * this.targetX, smoothingX)
      this.y = mix(this.y, pose.y - viewportHeight * targetY, smoothingY)
    } else {
      this.x = mix(this.x, -viewportWidth * this.targetX, idleSmoothing)
    }
    this.shake *= this.shakeDecay
    if (this.shake < 0.1) this.shake = 0
  }

  addShake(amount) {
    this.shake = Math.min(16, this.shake + amount)
  }

  // Random offset applied this frame, from the current shake energy.
  shakeOffset() {
    if (this.shake === 0) return { x: 0, y: 0 }
    return { x: (Math.random() - 0.5) * this.shake, y: (Math.random() - 0.5) * this.shake }
  }

  reset() {
    this.x = 0
    this.y = 0
    this.targetX = this.cfg.baseTargetX
    this.shake = 0
  }
}
