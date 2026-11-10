export const RUNNING_GEAR = {
    hullPixelLength: 400, // On-screen hull length (px); scale anchor for the vehicle

    wheelCount: 7,
    wheelRadiusRatio: 0.0556, // Road-wheel radius / hullPixelLength
    wheelSpacingRatio: 0.1173, // Wheel center spacing / hullPixelLength

    idlerRadiusRatio: 0.0532, // Idler radius / hullPixelLength
    frontIdlerXRatio: 0.448, // Front idler X offset from hull center / hullPixelLength
    rearIdlerXRatio: -0.413, // Rear idler X offset from hull center / hullPixelLength
    idlerRiseRatio: 0.0526, // Idler lift above road-wheel center line / hullPixelLength

    hubDropRatio: 0.0453, // Road-wheel center drop below hull skirt / hullPixelLength
    sponsonDropRatio: 0.6, // Sponson drop below wheel top / wheel diameter
    sponsonTaperRatio: 0.18, // Sponson bottom inset per side / sponson width

    restLengthRatio: 0.055, // Unloaded spring length / hullPixelLength
    fullyCompressedRatio: 0.01, // Bump-stop spring length / hullPixelLength
    fullyExtendedRatio: 0.1, // Max droop spring length / hullPixelLength

    trackClearance: 3.8, // Belt offset outside wheel radius (px)
    trackPadLength: 14, // Target arc spacing between track pads (px)
    trackMinPads: 24,
    trackScrollRate: 16, // Belt scroll speed per ground speed (px/s per px/frame)

    barrelLengthRatio: 0.29, // Muzzle projection past hull nose / hullPixelLength
    barrelRadiusRatio: 0.017, // Barrel half-thickness / hullPixelLength
}

export const CONFIG = {
    springLinearStiffness: 50, // Linear spring rate (static wheel load multiple)
    springProgressiveStiffness: 2, // Cubic spring rate near bump stop (static wheel load multiple)
    shockAbsorberDamping: 3, // Vertical damping coefficient (force per px/frame)
    wheelExtensionRate: 1.8, // Max wheel extension speed (px per 60 Hz frame)

    bodyDensity: 1.0, // Hull mass per area (Matter density units)
    bodyRotationInertiaScale: 0.6, // Multiplier on hull rotational inertia
    airResistance: 0.001, // Matter frictionAir coefficient (0..1)

    accelerationForce: 0.3, // Forward drive force (vehicle weight multiple)
    brakingForce: 0.7, // Braking force (vehicle weight multiple)
    reverseForce: 0.1, // Reverse force (vehicle weight multiple)
    maximumSpeed: 40, // Top forward speed (px/frame at 60 Hz)
    slowSpeed: 4.5, // Autonomous cruise speed when unfocused (px/frame at 60 Hz)

    driveGripLimit: 0.8, // Max drive traction / wheel normal force
    brakeGripLimit: 0.8, // Max brake traction / wheel normal force
    rollingResistanceGripLimit: 0.7, // Max coasting drag / wheel normal force

    coastingDrag: 0.009, // Coasting drag per px/frame (vehicle weight multiple)
    tractionArm: 100, // Vertical lever arm below wheel contact for pitch torque (px)
    gravity: 1, // Vertical gravity acceleration (Matter units)
}

export const RENDER = {
    tracks: {
        scale: 1,
        rotation: 0, // Baseline track orientation offset (rad)
    },
    body: {
        offsetX: -10, // Hull X offset relative to tracks (px)
        offsetY: 0, // Hull Y offset relative to tracks (px)
        scale: 1.05,
        rotation: -0.025, // Hull pitch offset relative to tracks (rad, negative = nose-up)
    },
}

export const CAMERA = {
    baseTargetX: 0.42, // Resting tank screen X (0..1 of viewport width)
    forwardTargetX: 0.05, // Full-speed forward screen X (0..1 of viewport width)
    reverseTargetX: 0.7, // Full-speed reverse screen X (0..1 of viewport width)
    targetY: 0.52, // Tank screen Y (0..1 of viewport height)

    cruiseSpeed: 18, // Speed for full camera framing shift (px/frame)

    smoothingX: 0.12, // Horizontal lerp factor per 60 Hz frame (0..1)
    smoothingY: 0.08, // Vertical lerp factor per 60 Hz frame (0..1)
    idleSmoothing: 0.05, // Camera lerp factor when no tank is active (0..1)
    framingSmoothing: 0.025, // Target-X framing lerp factor per 60 Hz frame (0..1)
}

export const PALETTE = {
    grassTop: '#8f8155',
    grassDark: '#6b603c',
    track: '#332f28',
    trackHi: '#5c574c',
    hull: ['#efe0c2', '#e0cba6', '#cdb88e'],
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

export const BACKGROUND = {
    octaves: 6,
    persistence: 0.55, // Amplitude multiplier per fBm octave
    lacunarity: 2.4, // Frequency multiplier per fBm octave
    sharpness: 2.0, // Ridge exponent (>1 sharpens peaks)
    step: 8, // Horizontal ridge sampling step (px)

    sun: {
        x: 0.84, // Sun center X (0..1 of canvas width)
        y: 0.32, // Sun center Y (0..1 of canvas height)
        radius: 0.095, // Sun radius (0..1 of canvas height)
        color: '#fff2cf',
        glow: 'rgba(255,196,110,0.35)',
    },

    // Mountain layers from far to near; baseY and amplitude in 0..1 of canvas height, frequency in 1/px.
    layers: [
        { parallax: 0.05, baseY: 0.63, amplitude: 0.5, frequency: 0.0011, seed: 0, peak: '#785a37', base: '#9e7a4f' },
        { parallax: 0.13, baseY: 0.71, amplitude: 0.44, frequency: 0.0018, seed: 137, peak: '#563d24', base: '#7a5936' },
        { parallax: 0.3, baseY: 0.79, amplitude: 0.38, frequency: 0.0026, seed: 271, peak: '#3a2718', base: '#6b4a2c' },
    ],
}
