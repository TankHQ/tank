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
  // Road wheels: `count` of them, `radius` each, `spacing` apart. These ratios
  // are taken from the reference M1: spacing is ~2.75x the radius, so the wheels
  // have clear gaps between them (not overlapping).
  wheelCount: 7,
  wheelRadius: 22,
  wheelSpacing: 60,
  wheelMountY: -2, // mount height in body coords (y points down)

  // Idlers ("top wheels"). Each sits on a ray from the hull origin: `distance`
  // out from the wheel-centre line, `angle` above horizontal (radians). `side`
  // +1 = front, -1 = rear. Raise `angle` to lift an idler; change `distance` to
  // slide it inboard or outboard. Reference idlers are large (~1.4x road wheel)
  // and sit well above the wheel-centre line.
  idlers: [
    { distance: 226, angle: 0.145, radius: 31, side: 1 },
    { distance: 226, angle: 0.145, radius: 31, side: -1 },
  ],

  // Suspension travel, measured from the wheel mount down to the wheel centre.
  restLength: 38,
  fullyCompressed: 5,
  fullyExtended: 58,

  // Track belt.
  trackClearance: 3.2, // how far the belt sits outside the wheels
  trackPadLength: 8.5, // target arc spacing between track pads
  trackMinPads: 24,

  // Hull proportions, as fractions of the wheel radius. From the reference:
  // the deck sits ~4 radii above the wheel centres, the skirt bottom ~0.45
  // radii above them (so it covers the top of the idlers but clears the road
  // wheels), and the sponsons overhang the belt by `hullOverhangRatio`.
  deckAboveHub: 4.0,
  skirtAboveHub: 0.45,
  hullOverhangRatio: 0.9,
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

  // Skirt bottom and deck line come from the reference proportions, measured
  // against the wheel-centre line. The skirt sits just above the wheel centres
  // so it covers the top of the idlers while the road wheels stay exposed.
  const skirtY = wheelCentreY - rg.wheelRadius * rg.skirtAboveHub
  const deckY = wheelCentreY - rg.wheelRadius * rg.deckAboveHub

  const trackHalf = Math.max(...idlers.map((i) => Math.abs(i.x) + i.radius)) + rg.trackClearance
  const hullHalf = trackHalf + rg.wheelRadius * rg.hullOverhangRatio

  // Hull outline, following the reference side profile: a near-vertical rear
  // plate, a long flat deck, a short sloping glacis at the front and a straight
  // skirt bottom. `hullHalf` is the rear/front extent, `skirtY` the lower edge.
  const hullMaxX = hullHalf
  const hullMinX = -hullHalf
  const glacisStart = hullMaxX - rg.wheelRadius * 2.2
  const rawHull = [
    { x: hullMinX, y: skirtY }, // rear lower corner
    { x: hullMinX, y: deckY + rg.wheelRadius * 1.1 }, // rear plate
    { x: hullMinX + rg.wheelRadius * 0.55, y: deckY }, // deck rear corner
    { x: glacisStart, y: deckY }, // deck front
    { x: hullMaxX, y: deckY + rg.wheelRadius * 1.6 }, // glacis slope
    { x: hullMaxX, y: skirtY }, // front lower corner
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
  // ~9.8 m tracked vehicle (M1 hull length including sponsons). This turns
  // simulation pixels into real-world metres for the HUD. Derived from the gear.
  pixelsPerMetre: GEAR.hullPixelLength / 9.8,

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

// The turret is a visual overlay on the hull. Its outline is derived from the
// reference proportions: it spans from ~14% to ~93% of the hull length (nearly
// the whole hull, as on an M1) and rises ~1.9 wheel radii above the deck, with
// a long flat roof between a rear slope and a front slope.
export const TURRET = (() => {
  const L = BODY.length
  const R = RUNNING_GEAR.wheelRadius
  const rear = BODY.minX + L * 0.14
  const front = BODY.minX + L * 0.93
  const roofY = BODY.deckY - R * 1.9
  const roofRear = rear + R * 2.2
  const roofFront = front - R * 8.0
  return {
    points: [
      { x: rear, y: BODY.deckY },
      { x: rear + R * 0.6, y: BODY.deckY - R * 1.1 },
      { x: roofRear, y: roofY },
      { x: roofFront, y: roofY },
      { x: roofFront + R * 2.6, y: BODY.deckY - R * 1.1 },
      { x: front - R * 1.2, y: BODY.deckY - R * 0.4 },
      { x: front, y: BODY.deckY },
    ],
    roof: roofY,
  }
})()

// Where the barrel tip sits in body-local coords, for spawning shells. The gun
// axis sits just above the deck and the barrel projects well past the nose.
export const GUN = {
  muzzleX: hullMaxX + RUNNING_GEAR.wheelRadius * 8.8,
  muzzleY: BODY.deckY - RUNNING_GEAR.wheelRadius * 0.55,
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
  // Desert-camouflage bodywork (M1 Abrams tan). The reference is a pale sand
  // base with soft mid-tan and darker khaki fields, so keep the contrast gentle.
  hull: ['#efe0c2', '#e3cfa9', '#d3bd94'],
  camo: ['#e9d8b8', '#b99a6b', '#cbb185', '#a88a5c'],
  barrel: '#d3bd94',
  barrelHi: '#e8dcc0',
  hullLine: 'rgba(90,70,42,0.55)',
  wheel: '#dcc9a2',
  wheelRim: '#b39a70',
  wheelHub: '#b2422e',
}

export { hullMaxX, hullMinX, hullMinY, hullMaxY }
