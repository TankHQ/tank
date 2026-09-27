// A pool of short-lived visual particles (dust, muzzle flash, impact debris).
//
// Each particle is a simple point with velocity, gravity, lifetime and colour.
// The field owns them all; callers just spawn named kinds.
export class ParticleField {
    constructor() {
        this.particles = []
    }

    _spawn({ x, y, vx, vy, life, size, color, gravity }) {
        this.particles.push({
            x, y, vx, vy,
            life,
            max: life,
            size,
            color,
            gravity,
        })
    }

    // Exhaust puff trailing behind the hull.
    exhaust(x, y, groundSpeed) {
        this._spawn({
            x, y,
            vx: -groundSpeed * 0.25,
            vy: -0.6 - Math.random(),
            life: 0.7,
            size: 3 + Math.random() * 4,
            color: '#6f6a5a',
            gravity: -0.01,
        })
    }

    // Muzzle flash and smoke when the cannon fires.
    muzzleFlash(x, y, direction) {
        for (let i = 0; i < 26; i++) {
            const a = direction + (Math.random() - 0.5) * 0.9
            const speed = 3 + Math.random() * 9
            this._spawn({
                x, y,
                vx: Math.cos(a) * speed,
                vy: Math.sin(a) * speed,
                life: 0.35 + Math.random() * 0.3,
                size: 2 + Math.random() * 4,
                color: Math.random() < 0.5 ? '#ffd27a' : '#f38b3a',
                gravity: 0.02,
            })
        }
    }

    // Burst of debris where a shell lands.
    impact(x, y) {
        for (let i = 0; i < 14; i++) {
            this._spawn({
                x, y,
                vx: (Math.random() - 0.5) * 6,
                vy: -Math.random() * 5,
                life: 0.4 + Math.random() * 0.4,
                size: 2 + Math.random() * 4,
                color: Math.random() < 0.6 ? '#5a5140' : '#a58c5c',
                gravity: 0.03,
            })
        }
    }

    update(dt) {
        const step = dt / 1000
        for (let i = this.particles.length - 1; i >= 0; i--) {
            const p = this.particles[i]
            p.life -= step
            p.x += p.vx * 60 * step
            p.y += p.vy * 60 * step
            p.vy += p.gravity * 60 * step * 60
            if (p.life <= 0) this.particles.splice(i, 1)
        }
    }

    clear() {
        this.particles.length = 0
    }
}
