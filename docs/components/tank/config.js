// All tunable numbers and fixed geometry for the tank simulation.
//
// Forces are expressed as multiples of the vehicle's own weight (1.0 = the
// tank's weight), so the tuning reads the same regardless of how heavy the
// chassis ends up being. Matter integrates forces as `v += (F/m) * dt^2`, which
// is why the raw stiffness/damping numbers are small.

// -----------------------------------------------------------------------------
// RUNNING GEAR  (the single source of truth for the undercarriage)
// -----------------------------------------------------------------------------
// Everything to do with the undercarriage is derived from these numbers by
// `buildRunningGear` below:
//   * the road-wheel mounts and their spacing,
//   * the idler ("top wheel") mounts, each placed at a distance and an angle,
//   * the hull's skirt line, deck and overall length (the sponsons overhang the
//     belt by `hullOverhang`),
//   * the suspension travel, and
//   * the track belt, which is built to wrap whatever discs this produces.
// Change one number and the whole tank — visual and physical — follows.
export const RUNNING_GEAR = {
  // Road wheels: `count` of them, `radius` each, `spacing` apart.
  wheelCount: 7,
  wheelRadius: 22,
  wheelSpacing: 38,
  wheelMountY: -2, // mount height in body coords (y points down)

  // Idlers ("top wheels"). Each sits on a ray from the hull origin: `distance`
  // out from the wheel-centre line, `angle` above horizontal (radians). `side`
  // +1 = front, -1 = rear. Raise `angle` to lift an idler; change `distance` to
  // slide it inboard or outboard.
  idlers: [
    { distance: 154, angle: 0.17, radius: 21, side: 1 },
    { distance: 154, angle: 0.17, radius: 21, side: -1 },
  ],

  // Suspension travel, measured from the wheel mount down to the wheel centre.
  restLength: 38,
  fullyCompressed: 5,
  fullyExtended: 58,

  // Track belt.
  trackClearance: 3.2, // how far the belt sits outside the wheels
  trackPadLength: 8.5, // target arc spacing between track pads
  trackMinPads: 24,

  // Hull shape. `hullOverhang` is how far the sponsons project past the belt;
  // `deckY` is the flat top edge (body coords); the skirt bottom is derived to
  // cover the top half of the idlers while staying clear of the road wheels.
  hullOverhang: 30,
  deckY: -52,
}

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

// Derive every undercarriage dimension from RUNNING_GEAR.
function buildRunningGear(rg) {
  const span = (rg.wheelCount - 1) * rg.wheelSpacing
  const halfSpan = span / 2
  const wheelCentreY = rg.wheelMountY + rg.restLength

  const wheels = []
  for (let i = 0; i < rg.wheelCount; i++) {
    wheels.push({
      x: -halfSpan + i * rg.wheelSpacing,
      y: rg.wheelMountY,
      radius: rg.wheelRadius,
    })
  }

  // Idlers are placed on a ray: `distance` out from the hull centre and `angle`
  // above the wheel-centre line. Their inboard edge is kept clear of the
  // outermost road wheel, so the layout never overlaps whatever the wheels are.
  const roadWheelOuter = halfSpan + rg.wheelRadius
  const idlers = rg.idlers.map((id) => {
    const rawX = id.distance * Math.cos(id.angle)
    const x = Math.max(Math.abs(rawX), roadWheelOuter + id.radius * 0.2) * id.side
    return {
      x,
      y: wheelCentreY - id.distance * Math.sin(id.angle),
      radius: id.radius,
    }
  })

  // The skirt bottom sits at the idler centre line, so it hides the top half of
  // the idlers — but never below the road-wheel tops, which stay fully exposed.
  const roadWheelTop = wheelCentreY - rg.wheelRadius
  const skirtY = Math.min(
    Math.max(...idlers.map((i) => i.y)),
    roadWheelTop,
  )

  const trackHalf = Math.max(...idlers.map((i) => Math.abs(i.x) + i.radius)) + rg.trackClearance
  const hullHalf = trackHalf + rg.hullOverhang

  // Hull outline.
  const rawHull = [
    { x: -hullHalf, y: skirtY },
    { x: -hullHalf, y: rg.deckY + 30 },
    { x: -hullHalf + 30, y: rg.deckY },
    { x: hullHalf - 70, y: rg.deckY },
    { x: hullHalf, y: rg.deckY + 40 },
    { x: hullHalf, y: skirtY },
  ]

  // Matter recentres a body's vertices on their centroid and treats
  // `body.position` as that centroid. So the *whole* running gear must be
  // expressed in that same centroid-relative frame, or the wheels would end up
  // offset from the hull by the centroid. Recentre hull, mounts and idlers
  // together on the hull's centroid.
  const c = polygonCentroid(rawHull)
  const hullPoints = rawHull.map((p) => ({ x: p.x - c.x, y: p.y - c.y }))
  for (const w of wheels) {
    w.x -= c.x
    w.y -= c.y
  }
  for (const i of idlers) {
    i.x -= c.x
    i.y -= c.y
  }

  return {
    wheels,
    idlers,
    hullPoints,
    centroid: c,
    wheelCentreY: wheelCentreY - c.y,
    skirtY: skirtY - c.y,
    trackHalf,
    hullHalf,
    hullPixelLength: hullHalf * 2,
  }
}

export const GEAR = buildRunningGear(RUNNING_GEAR)

// Road wheels: mount position relative to the hull centre, and radius.
export const WHEEL_MOUNTS = GEAR.wheels

// Idler wheels ("top wheels"): fixed to the chassis (not sprung), placed at a
// distance and angle by RUNNING_GEAR.idlers.
export const IDLER_MOUNTS = GEAR.idlers

// Chassis outline, in body-local coordinates (y points down, as in Matter).
export const HULL_POINTS = GEAR.hullPoints

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
  // ~7.93 m tracked vehicle (M1 hull length). This is what turns simulation
  // pixels into real-world metres for the HUD. Derived from the running gear.
  pixelsPerMetre: GEAR.hullPixelLength / 7.93,

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
  suspensionRestLength: RUNNING_GEAR.restLength,
  suspensionFullyCompressedLength: RUNNING_GEAR.fullyCompressed,
  suspensionFullyExtendedLength: RUNNING_GEAR.fullyExtended,

  // How fast a wheel may stretch back down toward the ground, in px per 60fps
  // frame. This rebound lag is what lets the tank leave the ground over bumps.
  wheelExtensionRate: 1.8,

  // ---------------------------------------------------------------------------
  // BODY  (the chassis)
  // ---------------------------------------------------------------------------

  // Mass per unit area. Higher = harder to accelerate and push around.
  bodyDensity: 1,

  // The main dial for how much the body tilts on bumpy ground: the body's
  // rotational inertia is multiplied by this. 1.0 = calm and heavy,
  // 0.5 = lively, 0.2 = twitchy, below 0.1 it can flip.
  bodyRotationInertiaScale: 0.6,

  // Air resistance; also settles any spin once airborne.
  airResistance: 0.002,

  // ---------------------------------------------------------------------------
  // ENGINE AND BRAKES  (multiples of the vehicle's weight)
  // ---------------------------------------------------------------------------

  accelerationForce: 0.3, // forward push
  brakingForce: 0.5, // braking push
  reverseForce: 0.35, // reverse push
  maximumSpeed: 40, // top speed in px/frame (~89 km/h)

  // Grip: a wheel transmits at most `gripLimit * weight on that wheel` before
  // the track slips.
  driveGripLimit: 2.4,
  brakeGripLimit: 5.0,
  rollingResistanceGripLimit: 0.7,

  // How quickly the tank coasts to a stop with no throttle.
  coastingDrag: 0.08,

  // Gravity, in Matter's units.
  gravity: 1,
}

// -----------------------------------------------------------------------------
// BODY GEOMETRY  (derived from the hull outline; drives the renderer's art)
// -----------------------------------------------------------------------------
// The renderer draws the body as vector art rather than a sprite. These anchors
// are all derived from HULL_POINTS so the art follows any running-gear change.
const hullMaxX = Math.max(...HULL_POINTS.map((p) => p.x)) // nose
const hullMinX = Math.min(...HULL_POINTS.map((p) => p.x)) // tail
const hullMinY = Math.min(...HULL_POINTS.map((p) => p.y)) // deck line (y points down)
const hullMaxY = Math.max(...HULL_POINTS.map((p) => p.y)) // skirt line

export const BODY = {
  minX: hullMinX,
  maxX: hullMaxX,
  deckY: hullMinY,
  skirtY: hullMaxY,
  length: hullMaxX - hullMinX,
  depth: hullMaxY - hullMinY,
  // Where the road-wheel centres sit in body-local coords, so the renderer can
  // line the skirt up with the actual running gear.
  wheelCentreY: GEAR.wheelCentreY,
}

// The turret is a visual overlay on the hull. Its outline is expressed in
// y-up body-local coordinates (matching the renderer), sized from the hull.
export const TURRET = (() => {
  const L = BODY.length
  const rear = BODY.minX + L * 0.06
  const front = BODY.minX + L * 0.66
  const roof = BODY.deckY - L * 0.16 // above the deck (y points down)
  return {
    points: [
      { x: rear, y: BODY.deckY },
      { x: rear + 4, y: BODY.deckY - L * 0.09 },
      { x: rear + L * 0.07, y: roof },
      { x: front - L * 0.34, y: roof },
      { x: front - L * 0.26, y: BODY.deckY - L * 0.11 },
      { x: front - L * 0.08, y: BODY.deckY - L * 0.06 },
      { x: front, y: BODY.deckY },
    ],
    roof,
  }
})()

// Where the barrel tip sits in body-local coords, for spawning shells.
export const GUN = {
  muzzleX: hullMaxX + 210,
  muzzleY: BODY.deckY - 36, // above the deck, since y points down
}

// Render palette.
export const PALETTE = {
  sky: ['#1d2b40', '#3d4f68', '#9a6b46', '#e8b878'],
  sun: '#ffd9a0',
  farHill: '#4c5566',
  midHill: '#3f4a3f',
  nearHill: '#333a2f',
  grassTop: '#8f8155',
  grassDark: '#6b603c',
  soil: '#4a4029',
  soilDeep: '#332c1c',
  track: '#332f28',
  trackHi: '#5c574c',
  // Desert-camouflage bodywork (M1 Abrams tan).
  hull: ['#e0cda6', '#cdb489', '#ab9064'],
  camo: ['#e8d7b0', '#a8814f', '#c1a273', '#8f7247'],
  barrel: '#c6b085',
  barrelHi: '#dcc8a0',
  hullLine: 'rgba(70,52,28,0.65)',
  wheel: '#c9b58c',
  wheelRim: '#8c7648',
  wheelHub: '#b2422e',
}

export { hullMaxX, hullMinX, hullMinY, hullMaxY }
