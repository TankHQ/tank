import { CAMERA } from './config.js'
import { clamp, mix } from './util.js'

// Smoothly follows a target point. It is fed the *interpolated* render position
// so the tank and the world stay locked together on high-refresh displays.
//
// All smoothing is time-based, not per-frame. The `*Smoothing` values in config
// are the fraction of the gap closed in one 60 Hz frame (16.667ms); each frame
// converts that to the fraction for its own elapsed time. This matters two ways:
//   * the camera feels the same at 60Hz and 144Hz, and
//   * irregular frame timing (e.g. Firefox's timer jitter) no longer makes the
//     camera-to-tank lag oscillate, which showed up as the tank flicking between
//     two screen positions on alternating frames.
//
// The horizontal framing also reacts to speed. The tank's screen position is
// `viewportWidth * targetX`, so a smaller `targetX` places it further left:
//   * moving forward, `targetX` eases toward `forwardTargetX`, leaving most of
//     the screen for the road ahead and only a little space behind the tank;
//   * reversing, it eases toward `reverseTargetX`, showing the road behind;
//   * stopped, it eases back to `baseTargetX` (the centred start).
// The easing is deliberately slow, so the shift is progressive rather than
// snapping with the throttle. All the numbers live in `CAMERA` (config.js).

const BASE_FRAME_MS = 1000 / 60

// Fraction of the remaining gap to close this frame, given a per-60Hz-frame
// smoothing factor and the frame's real duration in milliseconds.
function smoothFactor(perFrame, dt) {
  return 1 - Math.pow(1 - perFrame, dt / BASE_FRAME_MS)
}

export class Camera {
  constructor(cfg = CAMERA) {
    this.cfg = cfg
    this.x = 0
    this.y = 0
    this.targetX = cfg.baseTargetX
    this.shake = 0
    this.shakeDecay = 0.88
  }

  // `dt` is the frame's wall-clock time in ms; `speed01` is the tank's forward
  // speed as a fraction of the cruise speed (negative when reversing), used to
  // bias the framing.
  follow(pose, viewportWidth, viewportHeight, speed01 = 0, dt = BASE_FRAME_MS) {
    const { baseTargetX, forwardTargetX, reverseTargetX, targetY, framingSmoothing, smoothingX, smoothingY, idleSmoothing } = this.cfg
    const safeDt = clamp(dt, 1, 100)
    const kFraming = smoothFactor(framingSmoothing, safeDt)
    const kX = smoothFactor(smoothingX, safeDt)
    const kY = smoothFactor(smoothingY, safeDt)
    const kIdle = smoothFactor(idleSmoothing, safeDt)

    const v = clamp(speed01, -1, 1)
    const desired = v >= 0
      ? mix(baseTargetX, forwardTargetX, v)
      : mix(baseTargetX, reverseTargetX, -v)
    this.targetX = mix(this.targetX, desired, kFraming)

    if (pose) {
      this.x = mix(this.x, pose.x - viewportWidth * this.targetX, kX)
      this.y = mix(this.y, pose.y - viewportHeight * targetY, kY)
    } else {
      this.x = mix(this.x, -viewportWidth * this.targetX, kIdle)
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
