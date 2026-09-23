// Projectile shells fired by the cannon, and their impacts.
export class ShellManager {
  constructor({ Matter, world, collisionCategories, particles }) {
    this.Matter = Matter
    this.world = world
    this.categories = collisionCategories
    this.particles = particles
    this.shells = []
    this._lifeSeconds = 2.6
    this._speed = 30
  }

  // Fired from a muzzle position/direction (see Tank.muzzle).
  launch({ x, y, dir }) {
    const { Bodies, Body, Composite } = this.Matter
    const body = Bodies.circle(x, y, 5, {
      density: 0.004,
      friction: 0.3,
      restitution: 0.35,
      frictionAir: 0.0015,
      label: 'shell',
      collisionFilter: { category: this.categories.shell, mask: this.categories.ground },
    })
    Body.setVelocity(body, { x: Math.cos(dir) * this._speed, y: Math.sin(dir) * this._speed })
    Composite.add(this.world, body)
    this.shells.push({ body, life: this._lifeSeconds })
    this.particles.muzzleFlash(x, y, dir)
  }

  update(dt) {
    for (let i = this.shells.length - 1; i >= 0; i--) {
      const shell = this.shells[i]
      shell.life -= dt / 1000
      if (shell.life <= 0) {
        this.particles.impact(shell.body.position.x, shell.body.position.y)
        this.Matter.Composite.remove(this.world, shell.body, true)
        this.shells.splice(i, 1)
      }
    }
  }

  clear() {
    for (const shell of this.shells) this.Matter.Composite.remove(this.world, shell.body, true)
    this.shells.length = 0
  }
}
