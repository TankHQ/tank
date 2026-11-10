import { RENDER } from './config.js'
import { BODY } from './geometry.js'

// 2D affine matrix [a, b, c, d, e, f]: x' = a*x + c*y + e, y' = b*x + d*y + f.
const translate = (tx, ty) => [1, 0, 0, 1, tx, ty]

const rotate = (a) => {
    const c = Math.cos(a)
    const s = Math.sin(a)
    return [c, s, -s, c, 0, 0]
}

const scale = (s) => [s, 0, 0, s, 0, 0]

function compose(a, b) {
    return [
        a[0] * b[0] + a[2] * b[1],
        a[1] * b[0] + a[3] * b[1],
        a[0] * b[2] + a[2] * b[3],
        a[1] * b[2] + a[3] * b[3],
        a[0] * b[4] + a[2] * b[5] + a[4],
        a[1] * b[4] + a[3] * b[5] + a[5],
    ]
}

const chain = (...ms) => ms.reduce(compose)

const tankZero = (pose) => compose(translate(pose.x, pose.y), rotate(pose.angle))

// World-space running-gear transform around the hull pose origin (px, rad).
export function tracksMatrix(pose) {
    const { scale: s, rotation } = RENDER.tracks
    return chain(
        translate(pose.x, pose.y),
        rotate(rotation),
        scale(s),
        translate(-pose.x, -pose.y),
    )
}

// Local-to-world body transform pivoting around the rear skirt corner (px, rad).
export function bodyMatrix(pose) {
    const { offsetX, offsetY, scale: s, rotation } = RENDER.body
    const pivotX = BODY.minX
    const pivotY = BODY.skirtY
    return chain(
        tankZero(pose),
        rotate(RENDER.tracks.rotation),
        translate(offsetX, offsetY),
        translate(pivotX, pivotY),
        rotate(rotation),
        scale(s),
        translate(-pivotX, -pivotY),
    )
}

// Total world-space body angle (rad).
export function bodyDirection(pose) {
    return pose.angle + RENDER.tracks.rotation + RENDER.body.rotation
}

export function apply(m, x, y) {
    return { x: m[0] * x + m[2] * y + m[4], y: m[1] * x + m[3] * y + m[5] }
}
