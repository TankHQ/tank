// All tunable numbers and fixed geometry for the tank simulation.
//
// Forces are expressed as multiples of the vehicle's own weight (1.0 = the
// tank's weight), so the tuning reads the same regardless of how heavy the
// chassis ends up being. Matter integrates forces as `v += (F/m) * dt^2`, which
// is why the raw stiffness/damping numbers are small.

export const CONFIG = {
  // World scale: the hull spans 218 px between HULL_POINTS extremes (x -110
  // to +108), which represents a ~7.5 m tracked vehicle. This is what turns
  // simulation pixels into real-world metres for the HUD and tuning.
  pixelsPerMetre: 218 / 7.5,

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

  // Suspension geometry in pixels, from the wheel mount to the wheel centre.
  // restLength      : where the wheel sits when parked (spring partly loaded).
  // fullyCompressed : the hard limit; the body can never sink past this.
  // fullyExtended   : the longest the wheel can hang when the ground drops away.
  suspensionRestLength: 38,
  suspensionFullyCompressedLength: 5,
  suspensionFullyExtendedLength: 58,

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
  bodyRotationInertiaScale: 1.0,

  // Air resistance; also settles any spin once airborne.
  airResistance: 0.002,

  // ---------------------------------------------------------------------------
  // ENGINE AND BRAKES  (multiples of the vehicle's weight)
  // ---------------------------------------------------------------------------

  accelerationForce: 0.1,  // forward push (~0.1 g, heavy)
  brakingForce: 0.35,      // braking push
  reverseForce: 0.12,      // reverse push
  maximumSpeed: 9,         // top speed in px/frame (~67 km/h)

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

// Chassis outline, in body-local coordinates (y points down, as in Matter).
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
