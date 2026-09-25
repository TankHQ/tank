// Tunable settings for the tank simulation. This file holds *only* raw values —
// numbers, strings and small literal objects. Nothing here is computed: anything
// derived from these settings lives in `geometry.js` (the tank's shape and
// running gear) or beside the code that uses it.
//
// Forces are expressed as multiples of the vehicle's own weight (1.0 = the
// tank's weight), so the tuning reads the same regardless of how heavy the
// chassis ends up being. Matter integrates forces as `v += (F/m) * dt^2`, which
// is why the raw stiffness/damping numbers are small.

// -----------------------------------------------------------------------------
// RUNNING GEAR
// -----------------------------------------------------------------------------
// Undercarriage dimensions, all expressed as ratios of the hull's on-screen
// length (`hullPixelLength`) so the running gear and the SVG body always scale
// together. `geometry.js` turns these into actual pixel positions.
//
//   road wheels : `wheelCount` in a row, `wheelSpacingRatio` apart
//   idlers      : one at each end, offset `frontIdlerXRatio` / `rearIdlerXRatio`
//                 from the hull centre and lifted `idlerRiseRatio` above the
//                 wheel-centre line
export const RUNNING_GEAR = {
    // The hull's on-screen length, in simulation pixels. The scale anchor for the
    // whole tank: change this one number and the entire vehicle resizes.
    hullPixelLength: 448,

    // How many metres that hull length represents, for the HUD's real-world units.
    hullLengthMetres: 9.8,

    wheelCount: 7,
    wheelRadiusRatio: 0.0556,
    wheelSpacingRatio: 0.1173,

    idlerRadiusRatio: 0.0532,
    frontIdlerXRatio: 0.448, // offset from hull centre, forward
    rearIdlerXRatio: -0.413, // offset from hull centre, aft
    idlerRiseRatio: 0.0526, // idler centre above the road-wheel centre line

    hubDropRatio: 0.0453, // road-wheel centre below the hull skirt line

    // The lower hull behind the wheels (the sponson/side-skirt). Its bottom sits
    // this fraction of a wheel diameter below the top of the wheels, so it hangs
    // down over the upper part of the road wheels. 0.25-0.33 looks right.
    sponsonDropRatio: 0.3,

    restLengthRatio: 0.055,
    fullyCompressedRatio: 0.01,
    fullyExtendedRatio: 0.1,

    // Track belt.
    trackClearance: 3.8, // how far the belt sits outside the wheels
    trackPadLength: 14, // target arc spacing between track pads
    trackMinPads: 24,
    trackScrollRate: 16, // track surface speed per px/frame of ground speed

    // Cannon, measured from the turret's gun anchor (see silhouette.js) and the
    // hull nose.
    barrelLengthRatio: 0.29, // how far the muzzle projects past the nose
    barrelRadiusRatio: 0.017, // barrel half-thickness
}

// -----------------------------------------------------------------------------
// CONFIG  (chassis, engine, environment)
// -----------------------------------------------------------------------------
export const CONFIG = {
    // ---------------------------------------------------------------------------
    // SUSPENSION  (how the wheels/springs hold the body up)
    // ---------------------------------------------------------------------------

    // Spring force as a multiple of the weight resting on one wheel:
    //   F = staticLoad * (springLinearStiffness * u + springProgressiveStiffness * u^3)
    // where u is compression as a fraction of full travel (0 = fully extended,
    // 1 = fully compressed). The linear term sets the soft ride; the progressive
    // term makes the spring much stiffer near the bump stop.
    springLinearStiffness: 25,
    springProgressiveStiffness: 5,

    // Shock absorbers. Higher = tauter and heavier; too high and bumps stop
    // being absorbed at all.
    shockAbsorberDamping: 3,

    // How fast a wheel may stretch back down toward the ground, in px per 60fps
    // frame. This rebound lag is what lets the tank leave the ground over bumps.
    wheelExtensionRate: 1.8,

    // ---------------------------------------------------------------------------
    // BODY  (the chassis)
    // ---------------------------------------------------------------------------

    // Mass per unit area. Higher = harder to accelerate and push around.
    bodyDensity: 1.0,

    // The main dial for how much the body tilts on bumpy ground: the body's
    // rotational inertia is multiplied by this. 1.0 = calm and heavy,
    // 0.5 = lively, 0.2 = twitchy, below 0.1 it can flip.
    bodyRotationInertiaScale: 0.6,

    // Air resistance; also settles any spin once airborne.
    airResistance: 0.001,

    // ---------------------------------------------------------------------------
    // ENGINE AND BRAKES  (multiples of the vehicle's weight)
    // ---------------------------------------------------------------------------

    accelerationForce: 0.3, // forward push
    brakingForce: 0.7, // braking push
    reverseForce: 0.1, // reverse push
    maximumSpeed: 40, // top speed in px/frame

    // Grip: a wheel transmits at most `gripLimit * weight on that wheel` before
    // the track slips.
    driveGripLimit: 0.8,
    brakeGripLimit: 0.8,
    rollingResistanceGripLimit: 0.7,

    // How quickly the tank coasts to a stop with no throttle.
    coastingDrag: 0.009,

    // Traction (drive / brake / reverse force) is applied at a point this far
    // *below* the wheel contact, i.e. below the track. Applying the force low gives
    // it a moment arm about the centre of mass, so the linear push is unchanged but
    // the hull pitches hard: throttle lifts the nose (counter-clockwise, weight
    // back), braking dives it (clockwise, weight forward). Only applied through
    // wheels that are touching the ground, so it does nothing in mid-air.
    //
    // It is deliberately strong. Measured on flat ground, ~100 gives roughly 4-5x
    // the baseline pitch under power and ~10x on the brakes; beyond ~115 the torque
    // overcomes the springs and the tank can tumble over bumps, so this is set just
    // under that with margin.
    tractionArm: 100,

    // Gravity, in Matter's units.
    gravity: 1,
}

// -----------------------------------------------------------------------------
// RENDER TRANSFORM  (how the art is placed relative to the tank's physics pose)
// -----------------------------------------------------------------------------
//   tracks  - the belt, road wheels and idlers. No offset (their zero is their
//             own pose), only a `scale` and a `rotation`. The tracks' rotation is
//             the tank's baseline orientation: it is applied to the body too, so
//             the body is oriented *relative to the tracks*.
//   body    - the hull, turret and cannon: an XY `offset`, its own `scale`, and a
//             `rotation` added on top of the tracks'. It rotates about the rear
//             of the hull (computed in transform.js).
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
    forwardTargetX: 0.05,
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

// -----------------------------------------------------------------------------
// PALETTE
// -----------------------------------------------------------------------------
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
    // Panel/hatch line work and the small rectangular fittings (bins, guards).
    panel: 'rgba(90,70,42,0.5)',
    panelFill: '#d8c39c',
    panelFillDark: '#b89b6e',
    barrel: '#cdb88e',
    barrelHi: '#e8dcc0',
    hullLine: 'rgba(90,70,42,0.55)',
    wheel: '#dcc9a2',
    wheelRim: '#b39a70',
    wheelHub: '#b2422e',
}

// -----------------------------------------------------------------------------
// BACKGROUND  (sunset sky and mountain ranges)
// -----------------------------------------------------------------------------
//   sky    - top-to-bottom gradient stops (deep dusk -> warm gold at the horizon)
//   sun    - position/colour of the low sun and its glow
//   layers - mountain ranges, far to near. `parallax` is how fast a range scrolls
//            relative to the camera, `baseY` where its foot sits (fraction of
//            canvas height), `amplitude` its height, `frequency` the horizontal
//            scale of its peaks, and `seed` offsets it into a different patch of
//            the noise field. Each is filled with a vertical `peak` (top) ->
//            `base` (foot) gradient.
export const BACKGROUND = {
    octaves: 6,
    persistence: 0.55,
    lacunarity: 2.4,
    sharpness: 2.0, // >1 narrows the peaks, <1 rounds them
    step: 8, // horizontal sampling resolution in px

    sky: ['#241c1a', '#4a3220', '#8a4f22', '#c07a2e', '#e6a94e', '#f7d488'],

    sun: {
        x: 0.84, // fraction of canvas width
        y: 0.35, // fraction of canvas height (sits near the ridge, partly hidden)
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
