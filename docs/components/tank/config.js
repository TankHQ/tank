// All tunable numbers and fixed geometry for the tank simulation.
//
// Forces are expressed as multiples of the vehicle's own weight (1.0 = the
// tank's weight), so the tuning reads the same regardless of how heavy the
// chassis ends up being. Matter integrates forces as `v += (F/m) * dt^2`, which
// is why the raw stiffness/damping numbers are small.

export const CONFIG = {
  // World scale: the hull spans 412 px between HULL_POINTS extremes (x -206
  // to +206), which represents a ~7.93 m tracked vehicle (M1 hull length).
  // This is what turns simulation pixels into real-world metres for the HUD.
  pixelsPerMetre: 412 / 7.93,

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
  bodyRotationInertiaScale: 0.6,

  // Air resistance; also settles any spin once airborne.
  airResistance: 0.002,

  // ---------------------------------------------------------------------------
  // ENGINE AND BRAKES  (multiples of the vehicle's weight)
  // ---------------------------------------------------------------------------

  accelerationForce: 0.3, // forward push
  brakingForce: 0.5,       // braking push
  reverseForce: 0.35,      // reverse push
  maximumSpeed: 40,        // top speed in px/frame (~89 km/h)

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
// A deep slab: the long flat lower edge (y = ~36) is the side skirt, sitting
// just below the road-wheel centre line so it covers the top half of every
// wheel and the top run of the track. The nose and tail slope up to the deck.
// The hull deliberately overhangs the track at both ends (longer than the
// running gear), as on the reference M1: the sponsons project forward and aft
// of the idler wheels.
export const HULL_POINTS = [
  { x: -196, y: 14 },
  { x: -206, y: -6 },
  { x: -186, y: -52 },
  { x: 150, y: -52 },
  { x: 196, y: -30 },
  { x: 206, y: 14 },
]

// Road wheels: mount position relative to the hull centre, and radius. Seven of
// them, large and closely spaced, sitting below the skirt and fully exposed.
export const WHEEL_MOUNTS = (() => {
  const count = 7;
  const radius = 22;
  const distance = 36;
  const y = -2;

  const mounts = [];
  const startX = -((count - 1) * distance) / 2;

  for (let i = 0; i < count; i++) {
    mounts.push({
      x: startX + i * distance,
      y,
      radius
    });
  }

  return mounts;
})();

// Idler wheels ("top wheels"): fixed to the chassis (not sprung). Set at the
// extreme ends and raised, so the side skirt covers roughly their upper half.
export const IDLER_MOUNTS = [
  { x: -152, y: 10, radius: 21 },
  { x: 152, y: 10, radius: 21 },
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
