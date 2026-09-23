import { mix } from './util.js'

// Smoothly follows a target point. Uses exponential smoothing with a fixed
// per-step factor; it is fed the *interpolated* render position so the tank and
// the world stay locked together on high-refresh displays.
export class Camera {
  constructor(config = {}) {
    this.x = 0
    this.y = 0
    this.targetX = 0.42 // where the target sits across the viewport width
    this.targetY = 0.64 // and down the viewport height
    this.smoothingX = config.smoothingX ?? 0.12
    this.smoothingY = config.smoothingY ?? 0.08
    this.idleSmoothing = config.idleSmoothing ?? 0.05
    this.shake = 0
    this.shakeDecay = config.shakeDecay ?? 0.88
  }

  follow(pose, viewportWidth, viewportHeight) {
    if (pose) {
      const goalX = pose.x - viewportWidth * this.targetX
      const goalY = pose.y - viewportHeight * this.targetY
      this.x = mix(this.x, goalX, this.smoothingX)
      this.y = mix(this.y, goalY, this.smoothingY)
    } else {
      this.x = mix(this.x, -viewportWidth * this.targetX, this.idleSmoothing)
    }
    this.shake *= this.shakeDecay
    if (this.shake < 0.1) this.shake = 0
  }

  addShake(amount, max = 16) {
    this.shake = Math.min(max, this.shake + amount)
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
