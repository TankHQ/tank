import { CATEGORY } from './config.js'

// Projectile shells fired by the cannon, and their impacts.
export class ShellManager {
  constructor({ planck, world, particles }) {
    this.planck = planck
    this.world = world
    this.particles = particles
    this.shells = []
    this._lifeSeconds = 2.6
    this._speed = 30
  }

  // Fired from a muzzle position/direction (see Tank.muzzle).
  launch({ x, y, dir }) {
    const { Vec2, Circle } = this.planck
    const body = this.world.createDynamicBody({
      position: Vec2(x, y),
      // Continuous collision so a fast shell cannot tunnel through terrain.
      bullet: true,
      linearVelocity: Vec2(Math.cos(dir) * this._speed, Math.sin(dir) * this._speed),
      angularDamping: 0.1,
    })
    body.createFixture(new Circle(5), {
      density: 0.004,
      friction: 0.3,
      restitution: 0.35,
      filterCategoryBits: CATEGORY.shell,
      filterMaskBits: CATEGORY.ground,
    })
    this.shells.push({ body, life: this._lifeSeconds })
    this.particles.muzzleFlash(x, y, dir)
  }

  update(dt) {
    for (let i = this.shells.length - 1; i >= 0; i--) {
      const shell = this.shells[i]
      shell.life -= dt / 1000
      if (shell.life <= 0) {
        const p = shell.body.getPosition()
        this.particles.impact(p.x, p.y)
        this.world.destroyBody(shell.body)
        this.shells.splice(i, 1)
      }
    }
  }

  clear() {
    for (const shell of this.shells) this.world.destroyBody(shell.body)
    this.shells.length = 0
  }
}
