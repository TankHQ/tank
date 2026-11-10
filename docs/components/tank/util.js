export const TAU = Math.PI * 2
export const clamp = (value, min, max) => (value < min ? min : value > max ? max : value)
export const mix = (a, b, t) => a + (b - a) * t
export const distance = (a, b) => Math.hypot(a.x - b.x, a.y - b.y)

// Shortest-path angular interpolation (rad, t in 0..1).
export function mixAngle(a, b, t) {
    let d = (b - a) % TAU
    if (d > Math.PI) d -= TAU
    if (d < -Math.PI) d += TAU
    return a + d * t
}

// Transform local (localX, localY) (px) by origin (originX, originY) (px) and angle (rad).
export function toWorld(originX, originY, angle, localX, localY) {
    const cos = Math.cos(angle)
    const sin = Math.sin(angle)
    return {
        x: originX + cos * localX - sin * localY,
        y: originY + sin * localX + cos * localY,
    }
}

function hash(n) {
    const x = Math.sin(n * 127.1 + 311.7) * 43758.5453123
    return x - Math.floor(x)
}

// Deterministic 1D smoothstep value noise in 0..1.
export function valueNoise(x) {
    const i = Math.floor(x)
    const f = x - i
    const u = f * f * (3 - 2 * f)
    return mix(hash(i), hash(i + 1), u)
}
