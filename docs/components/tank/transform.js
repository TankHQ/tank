import { RENDER } from './config.js'
import { BODY } from './geometry.js'

// 2D affine transforms, in the same [a, b, c, d, e, f] form canvas uses:
//   x' = a*x + c*y + e
//   y' = b*x + d*y + f
//
// The renderer applies these with `ctx.transform(...)`; Tank applies `apply()`
// to a single point (for the muzzle). Sharing the builders keeps the drawn art
// and the shell spawn in exactly the same place.

export const translate = (tx, ty) => [1, 0, 0, 1, tx, ty]

export const rotate = (a) => {
  const c = Math.cos(a)
  const s = Math.sin(a)
  return [c, s, -s, c, 0, 0]
}

export const scale = (s) => [s, 0, 0, s, 0, 0]

// Compose two transforms: map p -> a(b(p)). Canvas `transform()` post-multiplies
// in the same order, so the renderer and this stay consistent.
export function compose(a, b) {
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

// The tank's "zero" frame: the physics pose of the hull.
const tankZero = (pose) => compose(translate(pose.x, pose.y), rotate(pose.angle))

// Running gear: an extra rotation and scale about the tank's zero point. The
// world-space discs (already carried through the hull pose) are turned and sized
// together. No offset: the tracks' zero is their own pose.
export function tracksMatrix(pose) {
  const { scale: s, rotation } = RENDER.tracks
  return chain(
    translate(pose.x, pose.y),
    rotate(rotation),
    scale(s),
    translate(-pose.x, -pose.y),
  )
}

// Body (hull, turret, cannon): the tank zero, the tracks' baseline rotation,
// then an XY offset, a rotation about `pivot` and a scale about that pivot.
// Rotating about the rear pivot keeps the back of the hull where it lands.
export function bodyMatrix(pose) {
  const { offsetX, offsetY, scale: s, rotation } = RENDER.body
  // Rotate about the rear of the hull, so the back of the body stays put.
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

// The body's total orientation: the hull angle plus the tracks' baseline plus
// the body's own rotation. Uniform scale does not change a direction.
export function bodyDirection(pose) {
  return pose.angle + RENDER.tracks.rotation + RENDER.body.rotation
}

// Apply a transform to a point.
export function apply(m, x, y) {
  return { x: m[0] * x + m[2] * y + m[4], y: m[1] * x + m[3] * y + m[5] }
}
