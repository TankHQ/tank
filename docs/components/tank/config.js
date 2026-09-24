// All tunable numbers and fixed geometry for the tank simulation.
//
// Forces are expressed as multiples of the vehicle's own weight (1.0 = the
// tank's weight), so the tuning reads the same regardless of how heavy the
// chassis ends up being. Matter integrates forces as `v += (F/m) * dt^2`, which
// is why the raw stiffness/damping numbers are small.
//
// The hull and turret outlines come straight from `silhouette.js` (transcribed
// from the supplied Inkscape reference), scaled into simulation pixels against
// the running gear below. So the body art and the physics always agree.

import { SVG_MM } from './silhouette.js'

// -----------------------------------------------------------------------------
// RUNNING GEAR  (the single source of truth for the undercarriage)
// -----------------------------------------------------------------------------
// Every dimension here is a ratio of the hull's on-screen length
// (`hullPixelLength`), measured directly from the reference side view. Keeping
// them as ratios means the running gear and the SVG body always scale together:
// the road wheels sit `wheelRadius` under the hull's skirt, the idlers ride
// `idlerRise` above the wheel-centre line, and so on.
//
//   road wheels : 7 in a row, `wheelSpacing` apart
//   idlers      : one at each end, offset `frontIdlerX` / `rearIdlerX` from the
//                 hull centre and lifted `idlerRise` above the wheel centres
export const RUNNING_GEAR = {
  // The hull's on-screen length. This is the scale anchor for the whole tank,
  // and every ratio below is relative to it, so changing this one number
  // resizes the entire vehicle proportionally.
  hullPixelLength: 448,

  // --- ratios of hullPixelLength (from the reference) ---
  wheelCount: 7,
  wheelRadiusRatio: 0.0556,
  wheelSpacingRatio: 0.1173,

  idlerRadiusRatio: 0.0532,
  frontIdlerXRatio: 0.448, // offset from hull centre, forward
  rearIdlerXRatio: -0.413, // offset from hull centre, aft
  idlerRiseRatio: 0.0526, // idler centre above the road-wheel centre line

  hubDropRatio: 0.0453, // road-wheel centre below the hull skirt line

  restLengthRatio: 0.055,
  fullyCompressedRatio: 0.01,
  fullyExtendedRatio: 0.1,

  // Track belt.
  trackClearance: 3.6, // how far the belt sits outside the wheels
  trackPadLength: 6.8, // target arc spacing between track pads
  trackMinPads: 24,

  // Cannon, measured from the turret's gun anchor (see silhouette.js) and the
  // hull nose.
  barrelLengthRatio: 0.29, // how far the muzzle projects past the nose
  barrelRadiusRatio: 0.017, // barrel half-thickness
}

const SV = SVG_MM

// Bounding box of the SVG hull, used to anchor the scale and the deck line.
const svgHullMinX = Math.min(...SV.hull.map((p) => p[0]))
const svgHullMaxX = Math.max(...SV.hull.map((p) => p[0]))
const svgHullMinY = Math.min(...SV.hull.map((p) => p[1])) // deck line (y points down)
const svgHullMaxY = Math.max(...SV.hull.map((p) => p[1])) // skirt / belly

// Area centroid of a polygon, matching how Matter recentres body vertices.
function polygonCentroid(pts) {
  let twiceArea = 0
  let cx = 0
  let cy = 0
  for (let i = 0; i < pts.length; i++) {
    const p = pts[i]
    const q = pts[(i + 1) % pts.length]
    const cross = p.x * q.y - q.x * p.y
    twiceArea += cross
    cx += (p.x + q.x) * cross
    cy += (p.y + q.y) * cross
  }
  twiceArea *= 0.5
  return { x: cx / (6 * twiceArea), y: cy / (6 * twiceArea) }
}

// Derive every undercarriage dimension from RUNNING_GEAR + the SVG silhouette.
function buildRunningGear(rg) {
  const L = rg.hullPixelLength

  // Pixels per SVG millimetre: the hull is scaled to `hullPixelLength`.
  const k = L / (svgHullMaxX - svgHullMinX)

  // SVG (mm) -> raw simulation frame (y down, deck at y = 0).
  const raw = (p) => ({ x: (p[0] - svgHullMinX) * k, y: (p[1] - svgHullMinY) * k })

  const hullPoints = SV.hull.map(raw)
  const hullPhysics = SV.hullPhysics.map(raw)
  const turret = SV.turret.map(raw)
  // Each camo field is a polygon; pair it with its source fill colour.
  const camo = SV.camo.map((field, i) => ({
    fill: SV.camoFills[i % SV.camoFills.length],
    points: field.map(raw),
  }))

  // Derived running-gear dimensions (all in simulation pixels).
  const wheelRadius = rg.wheelRadiusRatio * L
  const wheelSpacing = rg.wheelSpacingRatio * L
  const idlerRadius = rg.idlerRadiusRatio * L
  const hubDrop = rg.hubDropRatio * L
  const idlerRise = rg.idlerRiseRatio * L
  const restLength = rg.restLengthRatio * L

  // Deck = top of the hull; skirt = bottom. Both read off the scaled SVG.
  const skirtY = (svgHullMaxY - svgHullMinY) * k
  const hubCentreY = skirtY + hubDrop

  // Road wheel mounts, spread evenly about the hull centre. Built in the raw
  // SVG frame (hull spans 0..hullPixelLength), so they recentre with the hull.
  const span = (rg.wheelCount - 1) * wheelSpacing
  const halfSpan = span / 2
  const hullCentreX = L / 2
  const wheels = []
  for (let i = 0; i < rg.wheelCount; i++) {
    wheels.push({
      x: hullCentreX - halfSpan + i * wheelSpacing,
      y: hubCentreY - restLength,
      radius: wheelRadius,
    })
  }

  // Idlers sit at the ends, lifted above the wheel-centre line.
  const idlers = [
    { x: hullCentreX + rg.frontIdlerXRatio * L, y: hubCentreY - idlerRise, radius: idlerRadius },
    { x: hullCentreX + rg.rearIdlerXRatio * L, y: hubCentreY - idlerRise, radius: idlerRadius },
  ]

  // Matter recentres a body's vertices on their centroid and treats
  // `body.position` as that centroid, so the whole running gear must live in
  // that same centroid-relative frame.
  const c = polygonCentroid(hullPhysics)
  const rel = (p) => ({ x: p.x - c.x, y: p.y - c.y })
  const relArr = (arr) => arr.map(rel)

  const hull = relArr(hullPoints)
  const physics = relArr(hullPhysics)
  for (const w of wheels) { w.x -= c.x; w.y -= c.y }
  for (const i of idlers) { i.x -= c.x; i.y -= c.y }

  // The turret and camouflage are in the same frame as the hull, so they get the
  // same transform. The turret's `roof` is its top edge.
  const turretPoints = relArr(turret)
  const turretRoof = Math.min(...turretPoints.map((p) => p.y))

  // Barrel anchor and muzzle in the same local frame.
  const anchor = rel(raw(SV.gunAnchor))
  const noseX = Math.max(...hull.map((p) => p.x))
  const gun = {
    anchorX: anchor.x,
    anchorY: anchor.y,
    muzzleX: noseX + rg.barrelLengthRatio * L,
    muzzleY: anchor.y,
  }

  // Camouflage fields, recentred like the hull.
  const camoFields = camo.map((field) => ({ fill: field.fill, points: relArr(field.points) }))

  return {
    wheels,
    idlers,
    hullPoints: hull,
    hullPhysics: physics,
    turretPoints,
    turretRoof,
    camo: camoFields,
    centroid: c,
    deckY: -c.y, // deck line (svgHullMinY maps to y = 0 before recentring)
    skirtY: skirtY - c.y,
    wheelCentreY: hubCentreY - c.y,
    pitchRadius: wheelRadius,
    gun,
  }
}

export const GEAR = buildRunningGear(RUNNING_GEAR)

// Road wheels: mount position relative to the hull centre, and radius.
export const WHEEL_MOUNTS = GEAR.wheels

// Idler wheels ("top wheels"): fixed to the chassis (not sprung).
export const IDLER_MOUNTS = GEAR.idlers

// Chassis outline, in body-local coordinates (y points down, as in Matter).
export const HULL_POINTS = GEAR.hullPhysics

// The drawable hull outline (higher detail than the physics body).
export const HULL_ART = GEAR.hullPoints

// Track construction, wrapping whatever discs the running gear produces.
export const TRACK = {
  padLength: RUNNING_GEAR.trackPadLength,
  clearance: RUNNING_GEAR.trackClearance,
  scrollRate: 16, // track surface speed per px/frame of ground speed
  minPads: RUNNING_GEAR.trackMinPads,
}

// -----------------------------------------------------------------------------
// CONFIG  (chassis, engine, environment)
// -----------------------------------------------------------------------------
export const CONFIG = {
  // World scale: the hull spans `hullPixelLength` px, which represents a
  // ~9.8 m tracked vehicle (M1 hull length including sponsons). This turns
  // simulation pixels into real-world metres for the HUD.
  pixelsPerMetre: RUNNING_GEAR.hullPixelLength / 9.8,

  // ---------------------------------------------------------------------------
  // SUSPENSION  (how the wheels/springs hold the body up)
  // ---------------------------------------------------------------------------

  // Spring force as a multiple of the weight resting on one wheel:
  //   F = staticLoad * (springLinearStiffness * u + springProgressiveStiffness * u^3)
  // where u is compression as a fraction of full travel (0 = fully extended,
  // 1 = fully compressed). The linear term sets the soft ride; the progressive
  // term makes the spring much stiffer near the bump stop.
  springLinearStiffness: 22,
  springProgressiveStiffness: 0.15,

  // Shock absorbers. Higher = tauter and heavier; too high and bumps stop
  // being absorbed at all.
  shockAbsorberDamping: 3,

  // Suspension geometry, taken from the running gear so the mounts and the
  // wheel centres always agree.
  suspensionRestLength: RUNNING_GEAR.restLengthRatio * RUNNING_GEAR.hullPixelLength,
  suspensionFullyCompressedLength: RUNNING_GEAR.fullyCompressedRatio * RUNNING_GEAR.hullPixelLength,
  suspensionFullyExtendedLength: RUNNING_GEAR.fullyExtendedRatio * RUNNING_GEAR.hullPixelLength,

  // How fast a wheel may stretch back down toward the ground, in px per 60fps
  // frame. This rebound lag is what lets the tank leave the ground over bumps.
  wheelExtensionRate: 1.8,

  // ---------------------------------------------------------------------------
  // BODY  (the chassis)
  // ---------------------------------------------------------------------------

  // Mass per unit area. Higher = harder to accelerate and push around.
  bodyDensity: 0.8,

  // The main dial for how much the body tilts on bumpy ground: the body's
  // rotational inertia is multiplied by this. 1.0 = calm and heavy,
  // 0.5 = lively, 0.2 = twitchy, below 0.1 it can flip.
  bodyRotationInertiaScale: 0.4,

  // Air resistance; also settles any spin once airborne.
  airResistance: 0.002,

  // ---------------------------------------------------------------------------
  // ENGINE AND BRAKES  (multiples of the vehicle's weight)
  // ---------------------------------------------------------------------------

  accelerationForce: 0.5, // forward push
  brakingForce: 0.9, // braking push
  reverseForce: 0.35, // reverse push
  maximumSpeed: 50, // top speed in px/frame (~89 km/h)

  // Grip: a wheel transmits at most `gripLimit * weight on that wheel` before
  // the track slips.
  driveGripLimit: 2.4,
  brakeGripLimit: 5.0,
  rollingResistanceGripLimit: 0.7,

  // How quickly the tank coasts to a stop with no throttle.
  coastingDrag: 0.001,

  // Gravity, in Matter's units.
  gravity: 1,
}

// -----------------------------------------------------------------------------
// BODY GEOMETRY  (derived from the silhouette; drives the renderer's art)
// -----------------------------------------------------------------------------
const hullMinX = Math.min(...GEAR.hullPoints.map((p) => p.x))
const hullMaxX = Math.max(...GEAR.hullPoints.map((p) => p.x))
const hullMinY = Math.min(...GEAR.hullPoints.map((p) => p.y)) // deck line
const hullMaxY = Math.max(...GEAR.hullPoints.map((p) => p.y)) // skirt line

export const BODY = {
  minX: hullMinX,
  maxX: hullMaxX,
  deckY: hullMinY,
  skirtY: hullMaxY,
  length: hullMaxX - hullMinX,
  depth: hullMaxY - hullMinY,
  wheelCentreY: GEAR.wheelCentreY,
}

// The turret outline (visual overlay on the hull), plus its top edge.
export const TURRET = {
  points: GEAR.turretPoints,
  roof: GEAR.turretRoof,
}

// Rendering transform for the tank, expressed relative to the tank's "zero"
// position: the physics pose of the hull (`view.pose`). Nothing here affects the
// simulation, only how the art is placed.
//
//   tracks  - the belt, road wheels and idlers. They have no offset (their zero
//             is their own pose), only a `scale` and a `rotation`. The tracks'
//             rotation is the tank's baseline orientation: it is applied to the
//             body too, so the body is oriented *relative to the tracks*.
//   body    - the hull, turret and cannon. It has an XY `offset` (in the tank
//             frame), its own `scale`, and a `rotation` added on top of the
//             tracks'. It rotates about `pivotX/pivotY` (in body coordinates),
//             which lets the offset art line up with the running gear.
export const RENDER = {
  tracks: {
    scale: 1,
    rotation: 0,
  },
  body: {
    offsetX: -10,
    offsetY: 0,
    scale: 1.05,
    rotation: -0.025, // nose-up, relative to the tracks
    pivotX: BODY.minX, // rotate about the rear of the hull, so the back stays put
    pivotY: BODY.skirtY,
  },
}

// -----------------------------------------------------------------------------
// CAMERA  (framing and how the view follows the tank)
// -----------------------------------------------------------------------------
// The tank's screen position is `viewportWidth * targetX`, so a smaller value
// places it further left. The camera eases `targetX` from `baseTargetX` toward
// `forwardTargetX` when driving and `reverseTargetX` when reversing.
export const CAMERA = {
  // Where the tank sits across the viewport width (fraction) and down its
  // height. `baseTargetX` is the resting start; forward/reverse are the extremes
  // reached at full speed.
  baseTargetX: 0.42,
  forwardTargetX: 0.18,
  reverseTargetX: 0.7,
  targetY: 0.64,

  // How fast the camera reaches full forward/reverse framing: the tank speed
  // (px/frame) at which the shift is fully expressed. Lower = it tucks toward
  // the road ahead sooner.
  cruiseSpeed: 18,

  // How fast the camera moves to catch up with the tank (per frame, 0..1).
  // Higher = snappier and tighter; lower = looser, more trailing.
  smoothingX: 0.12,
  smoothingY: 0.08,
  idleSmoothing: 0.05,

  // How quickly the framing shift itself eases toward its target (per frame).
  framingSmoothing: 0.025,
}

// Where the barrel sits in body-local coords, for drawing and for spawning
// shells. The gun axis leaves the turret at `anchorX/Y` and projects past the
// nose to `muzzleX`.
export const GUN = {
  anchorX: GEAR.gun.anchorX,
  anchorY: GEAR.gun.anchorY,
  muzzleX: GEAR.gun.muzzleX,
  muzzleY: GEAR.gun.muzzleY,
  radius: RUNNING_GEAR.barrelRadiusRatio * RUNNING_GEAR.hullPixelLength,
}

// Camouflage fields painted over the hull, in the shared body-local frame.
export const CAMO = GEAR.camo

// Render palette.
export const PALETTE = {
  grassTop: '#8f8155',
  grassDark: '#6b603c',
  soil: '#4a4029',
  soilDeep: '#332c1c',
  track: '#332f28',
  trackHi: '#5c574c',
  // Desert-camouflage bodywork (M1 Abrams tan): a pale sand base with soft
  // mid-tan and darker khaki fields, taken from the reference.
  hull: ['#efe0c2', '#e0cba6', '#cdb88e'],
  barrel: '#cdb88e',
  barrelHi: '#e8dcc0',
  hullLine: 'rgba(90,70,42,0.55)',
  wheel: '#dcc9a2',
  wheelRim: '#b39a70',
  wheelHub: '#b2422e',
}

// -----------------------------------------------------------------------------
// BACKGROUND  (procedurally generated mountain range)
// -----------------------------------------------------------------------------
// A flat, vector-style sunset backdrop (see Background.js): one vertical sky
// gradient, a low sun, and layered mountain silhouettes.
//
//   sky    - top-to-bottom gradient stops (deep dusk -> warm gold at the horizon)
//   sun    - position/colour of the low sun and its glow. It sits near the ridge
//            line so the nearer ranges partly overlap it.
//   layers - mountain ranges, far to near. `parallax` is how fast a range scrolls
//            relative to the camera, `baseY` where its foot sits (fraction of
//            canvas height), `amplitude` its height, `frequency` the horizontal
//            scale of its peaks, and `seed` offsets it into a different patch of
//            the noise field. Each is filled with a vertical `peak` (top) ->
//            `base` (foot) gradient. Distant ranges are pale and scroll slowly;
//            near ones are dark and scroll faster.
export const BACKGROUND = {
  octaves: 6,
  persistence: 0.55,
  lacunarity: 2.4,
  sharpness: 2.0, // >1 narrows the peaks, <1 rounds them
  step: 8, // horizontal sampling resolution in px

  sky: ['#241c1a', '#4a3220', '#8a4f22', '#c07a2e', '#e6a94e', '#f7d488'],

  sun: {
    x: 0.64, // fraction of canvas width
    y: 0.6, // fraction of canvas height (sits near the ridge, partly hidden)
    radius: 0.095, // fraction of canvas height
    color: '#fff2cf',
    glow: 'rgba(255,196,110,0.55)',
  },

  layers: [
    // Farthest: tall and high, pale, slow. Foot higher on screen (smaller baseY).
    { parallax: 0.05, baseY: 0.74, amplitude: 0.5, frequency: 0.0011, seed: 0, peak: '#c9a15f', base: '#ecd3a0' },
    // Middle: intermediate height, mid tone.
    { parallax: 0.13, baseY: 0.82, amplitude: 0.44, frequency: 0.0018, seed: 137, peak: '#8a6234', base: '#c39a5e' },
    // Nearest: lower, darkest, fastest.
    { parallax: 0.3, baseY: 0.9, amplitude: 0.38, frequency: 0.0026, seed: 271, peak: '#3a2718', base: '#6b4a2c' },
  ],
}
