import { mix } from './util.js'

// Smoothly follows a target point. Uses exponential smoothing with a fixed
// per-step factor; it is fed the *interpolated* render position so the tank and
// the world stay locked together on high-refresh displays.
export class Camera {
  constructor() {
    this.x = 0
    this.y = 0
    // Where the target sits across the viewport width and down its height.
    this.targetX = 0.42
    this.targetY = 0.64
    this.smoothingX = 0.12
    this.smoothingY = 0.08
    this.idleSmoothing = 0.05
    this.shake = 0
    this.shakeDecay = 0.88
  }

  follow(pose, viewportWidth, viewportHeight) {
    if (pose) {
      this.x = mix(this.x, pose.x - viewportWidth * this.targetX, this.smoothingX)
      this.y = mix(this.y, pose.y - viewportHeight * this.targetY, this.smoothingY)
    } else {
      this.x = mix(this.x, -viewportWidth * this.targetX, this.idleSmoothing)
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
    this.shake = 0
  }
}
