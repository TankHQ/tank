// All tunable numbers and fixed geometry for the tank simulation.
//
// The simulation runs in pixels (planck's `Settings.lengthUnitsPerMeter` is left
// at 1). Speeds are expressed in pixels per 60 Hz frame; planck measures in
// pixels per second, so the Tank converts where it compares against these.
// Forces are expressed as multiples of the vehicle's own weight (1.0 = the
// tank's weight), so the tuning is independent of how heavy the chassis is.

// Collision categories. The hull is deliberately *not* a physical body (the
// suspension joints hold it up and real wheel bodies carry ground contact), so
// only wheels, shells and terrain have collision filters.
export const CATEGORY = {
  ground: 0x0001,
  wheel: 0x0002,
  shell: 0x0008,
}

export const CONFIG = {
  // ---------------------------------------------------------------------------
  // SUSPENSION  (how the wheel joints hold the body up)
  // ---------------------------------------------------------------------------

  // Wheel joint spring. `frequencyHz` is the bounce rate; `dampingRatio` < 1 is
  // springy, 1 is critical. These replace the hand-rolled spring/damper.
  suspensionFrequencyHz: 4.5,
  suspensionDampingRatio: 0.8,

  // Suspension geometry in pixels, from the wheel mount to the wheel centre.
  // restLength is where the wheel sits when parked (the joint's neutral point).
  // travel is the compression span used to scale the HUD gauge.
  suspensionRestLength: 38,
  suspensionTravel: 28,

  // Road wheel bodies (planck needs real wheels; the old build used virtual
  // ones). Density is per wheel; friction is against the terrain.
  wheelDensity: 0.6,
  wheelFriction: 1.2,

  // ---------------------------------------------------------------------------
  // BODY  (the chassis)
  // ---------------------------------------------------------------------------

  // Mass per unit area. Higher = harder to accelerate and push around.
  bodyDensity: 0.9,

  // The main dial for how much the body tilts on bumpy ground: the body's
  // rotational inertia is multiplied by this. 1.0 = calm and heavy,
  // 0.5 = lively, 0.2 = twitchy, below 0.1 it can flip.
  bodyRotationInertiaScale: 1.5,

  // Air resistance, and angular damping that settles any spin once airborne.
  airResistance: 0.02,
  angularResistance: 0.05,

  // ---------------------------------------------------------------------------
  // ENGINE AND BRAKES  (multiples of the vehicle's weight)
  // ---------------------------------------------------------------------------

  accelerationForce: 1.8, // forward push
  brakingForce: 1.2,      // braking push
  reverseForce: 0.9,      // reverse push
  maximumSpeed: 80,       // top speed in px/frame

  // Grip: a wheel transmits at most `gripLimit * weight on that wheel` before
  // the track slips.
  driveGripLimit: 6.0,
  brakeGripLimit: 9.0,
  rollingResistanceGripLimit: 1.5,

  // How quickly the tank coasts to a stop with no throttle.
  coastingDrag: 0.12,

  // ---------------------------------------------------------------------------
  // WEIGHT TRANSFER  (nose lifting / dipping under power and braking)
  // ---------------------------------------------------------------------------

  // Traction acts below the centre of mass. These levers scale how strongly the
  // resulting squat/dive shows. Separate because braking force far exceeds
  // engine force. 0 = none, higher = more pronounced.
  accelerationPitchLever: 10,
  brakingPitchLever: 12,

  // Gravity, in px/s^2.
  gravity: 981,
}

// Chassis outline, in body-local coordinates (y points down, as in planck).
export const HULL_POINTS = [
  { x: -108, y: 4 },
  { x: 108, y: 4 },
  { x: 92, y: -20 },
  { x: -60, y: -26 },
  { x: -96, y: -12 },
  { x: -110, y: -2 },
]

// Road wheels: mount position relative to the hull centre, and radius.
export const WHEEL_MOUNTS = [
  { x: -94, y: -2, radius: 16 },
  { x: -56, y: -2, radius: 16 },
  { x: -19, y: -2, radius: 16 },
  { x: 19, y: -2, radius: 16 },
  { x: 56, y: -2, radius: 16 },
  { x: 94, y: -2, radius: 16 },
]

// Idler wheels: fixed to the chassis (not sprung), they only shape the track.
export const IDLER_MOUNTS = [
  { x: -114, y: 6, radius: 18 },
  { x: 114, y: 6, radius: 18 },
]

// Track construction.
export const TRACK = {
  padLength: 8.5,   // target arc spacing between track pads
  clearance: 3.2,   // how far the belt sits outside the wheels
  scrollRate: 16,   // track surface speed per px/frame of ground speed
  minPads: 24,
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
  track: '#2a2825',
  trackHi: '#545049',
  hull: ['#6d6a4a', '#575339', '#3a3726'],
  wheel: '#26241f',
  wheelRim: '#4e4a40',
}
