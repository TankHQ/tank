// Tracked vehicle with raycast suspension.
//
// The chassis is a single rigid body. Each road wheel is a *virtual* wheel:
// a ray is cast straight down (world space) from the wheel mount to the
// terrain, and a spring/damper force is applied to the hull at the contact
// point. Because the rays do not rotate with the hull, the force distribution
// across the wheels changes as the hull pitches: compress the front pair and
// the back pair extend, producing a restoring torque. That makes the chassis
// stable without any artificial pitch controller, and lets genuine weight
// transfer appear — the contact forces and drive/brake traction act below the
// centre of mass, so it squats on launch and dives on the brakes for free.
//
// Wheels also lag on extension (limited rebound rate). When the terrain drops
// away the wheels take time to reach it, so at speed the tank can clear a hop
// instead of the wheels staying glued to the ground.
//
// Matter integrates forces as v += (F/m) * dt^2, so all force constants below
// are tiny compared to the pixel masses involved.

export const CFG = {
  // ---------------------------------------------------------------------------
  // SUSPENSION  (how the wheels/springs hold the body up)
  // ---------------------------------------------------------------------------

  // Spring force, expressed as a multiple of the weight resting on one wheel.
  // The total spring force is `staticLoad * (linear * u + progressive * u^3)`
  // where u is the compression as a fraction of full travel (0 = fully
  // extended, 1 = fully compressed). The linear term sets the soft ride feel;
  // the progressive term makes the spring much stiffer near the bump stop, so
  // the body gets pushed up harder and harder the more it sinks.
  springLinearStiffness: 20,
  springProgressiveStiffness: 0.001,

  // Damping: how strongly the springs resist being moved (shock absorbers).
  // 0 = bouncy, springy, oscillates for a long time.
  // 0.03 = settled and controlled (current).
  // 0.06 = taut and heavy, bumps are absorbed almost immediately.
  // Higher values weld the body to the terrain, so keep it modest.
  shockAbsorberDamping: 0.02,

  // Suspension geometry, in pixels, measured from the wheel mount to the wheel.
  // suspensionRestLength = where the wheel sits when the tank is parked (the
  //                         spring is partly compressed by the weight).
  // suspensionFullyCompressedLength = the hard limit. The wheel can never be
  //                         pushed closer than this, so the body can never sink
  //                         into the ground.
  // suspensionFullyExtendedLength = the longest the wheel can hang down when
  //                         the ground drops away underneath it.
  suspensionRestLength: 38,
  suspensionFullyCompressedLength: 10,
  suspensionFullyExtendedLength: 58,

  // How fast a wheel is allowed to stretch back down toward the ground when the
  // terrain falls away, in pixels per 60fps frame. This is the "rebound lag":
  // because the wheels cannot keep up with a sudden drop, the tank can launch
  // off big bumps at speed instead of staying glued to the ground.
  wheelExtensionRate: 1.5,

  // ---------------------------------------------------------------------------
  // BODY  (the chassis you see)
  // ---------------------------------------------------------------------------

  // How heavy the body is per unit of area. Higher = more mass, which makes it
  // harder to accelerate and push around.
  bodyDensity: 0.9,

  // THE MAIN DIAL for how much the body tilts in reaction to bumpy ground.
  // The body's rotational inertia (how hard it is to spin) is multiplied by
  // this. The long chassis is naturally very hard to rotate, so it rides
  // smoothly through bumps. Reduce this to let the same bumps throw the body
  // around more. 1.0 = calm and heavy, 0.5 = lively (current), 0.2 = twitchy,
  // below 0.1 it can flip over.
  bodyRotationInertiaScale: 1.5,

  // Air resistance on the body. Also gently settles any spin once the tank is
  // airborne. Higher = both linear and rotational motion bleed off faster.
  airResistance: 0.002,

  // ---------------------------------------------------------------------------
  // ENGINE AND BRAKES  (all expressed as multiples of the vehicle's own weight,
  // so 1.0 means "as much push as the tank weighs")
  // ---------------------------------------------------------------------------

  // Forward push while accelerating. Higher = quicker off the line.
  accelerationForce: 1.0,

  // Braking force. Higher = stops in a shorter distance.
  brakingForce: 0.4,

  // Push while reversing (deliberately weaker than forward).
  reverseForce: 0.55,

  // Top speed, in pixels per frame.
  maximumSpeed: 30,

  // Grip limits. A wheel can only transmit as much force as it is gripping:
  // force is capped at `gripLimit * <weight on that wheel>`. These set how much
  // of the weight can be turned into push before the track slips.
  driveGripLimit: 2.4,   // while accelerating
  brakeGripLimit: 5.0,   // while braking (brakes bite harder than the engine)
  rollingResistanceGripLimit: 0.7, // when coasting, how much drag the tracks give

  // How quickly the tank coasts to a stop when you release the throttle. Higher
  // = it slows down sooner on its own.
  coastingDrag: 0.35,

  // ---------------------------------------------------------------------------
  // WEIGHT TRANSFER  (the body dipping/lifting under power and braking)
  // ---------------------------------------------------------------------------

  // When you accelerate, the drive push is applied below the body's centre of
  // mass, which tips the nose up and squats the rear. These arms scale how
  // strongly that shows. They are separate because braking force is much larger
  // than engine force, so the same arm would make braking pitch far too much.
  // 0 = no visible effect, higher = more pronounced.
  accelerationPitchLever: 10, // nose lifts / rear squats when accelerating
  brakingPitchLever: 12,      // nose dives when braking

  // Gravity pull (Matter's units). Leave at 1 for normal gravity.
  gravity: 1,
}

export const HULL_PTS = [
  { x: -108, y: 4 },
  { x: 108, y: 4 },
  { x: 92, y: -20 },
  { x: -60, y: -26 },
  { x: -96, y: -12 },
  { x: -110, y: -2 },
]

const WHEELS_LOCAL = [
  { x: -94, y: -2, r: 16 },
  { x: -56, y: -2, r: 16 },
  { x: -19, y: -2, r: 16 },
  { x: 19, y: -2, r: 16 },
  { x: 56, y: -2, r: 16 },
  { x: 94, y: -2, r: 16 },
]

const IDLERS_LOCAL = [
  { x: -114, y: 6, r: 18 },
  { x: 114, y: 6, r: 18 },
]

const TAU = Math.PI * 2
const clamp = (v, a, b) => (v < a ? a : v > b ? b : v)
const dist = (a, b) => Math.hypot(a.x - b.x, a.y - b.y)
// Matter integrates velocity as v += (F/m) * dt^2. A damper force proportional
// to instantaneous velocity is only stable while c*dt^2/m < 2, which is far too
// weak to give a heavy, taut feel. Clamping the damper to what can cancel the
// relative velocity in a single step keeps the integrator stable at any c.
const DT2 = (1000 / 60) * (1000 / 60)

function convexHull(pts) {
  const p = pts.slice().sort((a, b) => a.x - b.x || a.y - b.y)
  if (p.length < 3) return p
  const cross = (o, a, b) => (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x)
  const lower = []
  for (const pt of p) {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], pt) <= 0) lower.pop()
    lower.push(pt)
  }
  const upper = []
  for (let i = p.length - 1; i >= 0; i--) {
    const pt = p[i]
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], pt) <= 0) upper.pop()
    upper.push(pt)
  }
  lower.pop()
  upper.pop()
  return lower.concat(upper)
}

export function createTank({ Matter, engine, world, groundY }) {
  const { Bodies, Body, Composite, Engine } = Matter

  let hull = null
  let wheels = []
  let alive = false
  let trackPhase = 0
  let belt = null

  const input = { w: false, s: false }

  function spawn(x) {
    reset()
    // Place the hull so every wheel rests at its natural length:
    // mount.y + restLength + wheelRadius = groundY  (all wheels share these).
    const ly = WHEELS_LOCAL[0].y
    const r = WHEELS_LOCAL[0].r
    hull = Bodies.fromVertices(x, groundY(x) - (ly + CFG.suspensionRestLength + r), [HULL_PTS], {
      density: CFG.bodyDensity,
      frictionAir: CFG.airResistance,
      label: 'hull',
      collisionFilter: { category: 0x0002, mask: 0 },
    })
    Body.setAngle(hull, 0)
    Body.setAngularVelocity(hull, 0)
    if (CFG.bodyRotationInertiaScale !== 1) Body.setInertia(hull, hull.inertia * CFG.bodyRotationInertiaScale)
    Composite.add(world, hull)
    wheels = WHEELS_LOCAL.map((w) => ({
      local: { x: w.x, y: w.y },
      r: w.r,
      currentLength: CFG.suspensionRestLength,
      compression: 0,
      spinAngle: 0,
      contact: false,
      contactX: 0,
      contactY: 0,
      normalForce: 0,
    }))
    alive = true
  }

  function reset() {
    if (hull) Composite.remove(world, hull, true)
    hull = null
    wheels = []
    alive = false
    belt = null
  }

  // Per-wheel static load is the hull's weight force divided across the wheels.
  function staticLoad() {
    return (hull.mass * CFG.gravity * 0.001) / WHEELS_LOCAL.length
  }

  // Suspension force for a given compression (restLength - length) and the hull's
  // downward velocity at the contact.
  //
  // The spring supports the weight: it grows steeply near the bump stop
  // (progressive). The damper is clamped so that, summed across all wheels, it
  // can remove at most the hull's current downward velocity in one step. That
  // keeps the explicit integrator stable at any damping strength while never
  // letting the dampers lift the chassis on their own - the spring must always
  // be the thing pushing the tank up, with gravity balancing it.
  function suspForce(compression, vDown) {
    const travel = CFG.suspensionRestLength - CFG.suspensionFullyCompressedLength
    const u = clamp(compression / travel, 0, 1.6)
    const spring = staticLoad() * (CFG.springLinearStiffness * u + CFG.springProgressiveStiffness * u * u * u)
    const wanted = CFG.shockAbsorberDamping * vDown
    const maxDamp = (hull.mass * Math.abs(vDown)) / (WHEELS_LOCAL.length * DT2)
    const damper = clamp(wanted, -maxDamp, maxDamp)
    return Math.max(0, spring + damper)
  }

  // Mount point of a wheel in world space (rotates with the hull).
  function mountOf(w) {
    const cos = Math.cos(hull.angle)
    const sin = Math.sin(hull.angle)
    return {
      x: hull.position.x + cos * w.local.x - sin * w.local.y,
      y: hull.position.y + sin * w.local.x + cos * w.local.y,
    }
  }

  // Free length at which the wheel would just touch the terrain, casting
  // straight down in world space. Terrain is a heightfield, so this is exact.
  function freeLen(w) {
    const m = mountOf(w)
    return groundY(m.x) - m.y - w.r
  }

  // Advance a wheel toward the ground, limited on extension (rebound lag).
  // Compression is instantaneous; extension takes time so the tank can leave
  // the ground over big bumps at speed.
  function solveWheel(w, maxExtensionThisStep) {
    const target = freeLen(w)
    let contact = false
    if (target < CFG.suspensionFullyCompressedLength) {
      // driven past the bump stop: hard stop, full force
      w.currentLength = CFG.suspensionFullyCompressedLength
      contact = true
    } else if (target > CFG.suspensionFullyExtendedLength) {
      // ground out of reach: extend toward it, rate limited (rebound lag)
      w.currentLength = Math.min(CFG.suspensionFullyExtendedLength, w.currentLength + maxExtensionThisStep)
      contact = false
    } else if (target <= w.currentLength) {
      // compressing: ground has risen to meet the wheel
      w.currentLength = target
      contact = true
    } else {
      // extending: lag behind the terrain
      w.currentLength = Math.min(target, w.currentLength + maxExtensionThisStep)
      contact = w.currentLength >= target - 0.001
    }
    syncWheel(w, contact)
    if (!contact) return
    const m = mountOf(w)
    // velocity of the hull at the mount, projected on world-down
    const pvy = hull.velocity.y + hull.angularVelocity * (m.x - hull.position.x)
    const vDown = pvy
    const F = suspForce(CFG.suspensionRestLength - w.currentLength, vDown)
    w.normalForce = F
    Body.applyForce(hull, { x: w.contactX, y: w.contactY }, { x: 0, y: -F })
  }

  // Refresh derived wheel fields from its current length.
  function syncWheel(w, contact) {
    w.compression = clamp((CFG.suspensionRestLength - w.currentLength) / (CFG.suspensionRestLength - CFG.suspensionFullyCompressedLength), 0, 1)
    w.contact = contact
    const m = mountOf(w)
    w.contactX = m.x
    w.contactY = m.y + w.currentLength
    w.normalForce = 0
  }

  // After the hull has been clamped, let wheels that the ground has risen into
  // compress instantly, so no wheel is ever rendered below the terrain. Wheels
  // extending (ground falling away) keep their lag.
  function settleWheels() {
    for (const w of wheels) {
      const target = freeLen(w)
      if (target < w.currentLength) {
        w.currentLength = target < CFG.suspensionFullyCompressedLength ? CFG.suspensionFullyCompressedLength : target
        syncWheel(w, true)
      } else if (!w.contact) {
        syncWheel(w, false)
      }
    }
  }

  // Apply the suspension for one step. Compression is resolved against the
  // pre-integration pose; extension is rate-limited by wheelExtensionRate * dt.
  function applySuspension(dt) {
    const maxExtensionThisStep = CFG.wheelExtensionRate * (dt / (1000 / 60))
    for (const w of wheels) solveWheel(w, maxExtensionThisStep)
  }

  // Guarantee no wheel penetrates past its hard stop. A spring can be
  // overpowered by a fast hit; this is a positional constraint, so it cannot.
  function enforceGround() {
    let push = 0
    for (const w of wheels) {
      const m = mountOf(w)
      const pen = m.y + CFG.suspensionFullyCompressedLength + w.r - groundY(m.x)
      if (pen > push) push = pen
    }
    if (push > 0) {
      hull.position.y -= push
      if (hull.velocity.y > 0) hull.velocity.y = 0
    }
  }

  function applyTraction() {
    const vx = hull.velocity.x
    const grounded = wheels.filter((w) => w.contact && w.normalForce > 0)
    if (!grounded.length) return
    const n = grounded.length

    let total = 0
    let cap = 0
    const W = hull.mass * 0.001
    if (input.w && !input.s) {
      total = CFG.accelerationForce * W * clamp(1 - vx / CFG.maximumSpeed, 0, 1)
      cap = CFG.driveGripLimit
    } else if (input.s && !input.w) {
      if (vx > 0.4) {
        total = -CFG.brakingForce * W * clamp(vx / 5, 0, 1)
        cap = CFG.brakeGripLimit
      } else {
        total = -CFG.reverseForce * W
        cap = CFG.driveGripLimit
      }
    } else {
      total = -vx * CFG.coastingDrag * W
      cap = CFG.rollingResistanceGripLimit
    }

    const per = total / n
    let applied = 0
    for (const w of grounded) {
      const limit = cap * w.normalForce
      const f = clamp(per, -limit, limit)
      applied += f
      Body.applyForce(hull, { x: w.contactX, y: w.contactY }, { x: f, y: 0 })
    }

    // Weight-transfer moment: traction acts below the centre of mass, so net
    // forward drive lifts the nose (negative torque) and braking dives it.
    // Applied as a torque so the suspension restoring moment balances it into
    // a steady squat/dive rather than accumulating.
    if (applied !== 0) {
      const arm = applied > 0 ? CFG.accelerationPitchLever : CFG.brakingPitchLever
      hull.torque += -applied * arm
    }
  }

  function spinWheels(dt) {
    const vx = hull.velocity.x
    for (const w of wheels) w.spinAngle += (vx / w.r) * (dt / 16.666)
  }

  function step(dt) {
    if (!alive) return
    applySuspension(dt)
    applyTraction()
    Engine.update(engine, dt)
    enforceGround()
    settleWheels()
    spinWheels(dt)
  }

  function idlerDiscs() {
    const cos = Math.cos(hull.angle)
    const sin = Math.sin(hull.angle)
    return IDLERS_LOCAL.map((i) => ({
      x: hull.position.x + cos * i.x - sin * i.y,
      y: hull.position.y + sin * i.x + cos * i.y,
      r: i.r,
    }))
  }

  // World-space pose of each wheel, derived from its current suspension length.
  function wheelPositions() {
    const cos = Math.cos(hull.angle)
    const sin = Math.sin(hull.angle)
    return wheels.map((w) => {
      const ly = w.local.y + w.currentLength
      return {
        x: hull.position.x + cos * w.local.x - sin * ly,
        y: hull.position.y + sin * w.local.x + cos * ly,
        radius: w.r,
        spinAngle: w.spinAngle,
        compression: w.compression,
        contact: w.contact,
      }
    })
  }

  // Track belt = convex hull of the wheel/idler discs, resampled at fixed arc
  // spacing so it drapes around the wheels and bends as a chain.
  function beltLoop() {
    if (!alive) return null
    const pts = []
    const circles = idlerDiscs()
    for (const w of wheelPositions()) circles.push({ x: w.x, y: w.y, r: w.radius })
    for (const c of circles) {
      const rr = c.r + 3.2
      const n = c.r > 16 ? 22 : 18
      for (let i = 0; i < n; i++) {
        const a = (i / n) * TAU
        pts.push({ x: c.x + Math.cos(a) * rr, y: c.y + Math.sin(a) * rr })
      }
    }
    const hullPts = convexHull(pts)
    if (hullPts.length < 4) return null

    let total = 0
    for (let i = 0; i < hullPts.length; i++) total += dist(hullPts[i], hullPts[(i + 1) % hullPts.length])
    const count = Math.max(24, Math.round(total / 8.5))
    const spacing = total / count
    const cum = [0]
    for (let i = 0; i < hullPts.length; i++) cum.push(cum[i] + dist(hullPts[i], hullPts[(i + 1) % hullPts.length]))

    const loop = []
    for (let k = 0; k < count; k++) {
      const target = k * spacing
      let s = 0
      while (s < hullPts.length - 1 && cum[s + 1] < target) s++
      const segLen = cum[s + 1] - cum[s] || 1
      const t = clamp((target - cum[s]) / segLen, 0, 1)
      const a = hullPts[s]
      const b = hullPts[(s + 1) % hullPts.length]
      loop.push({ x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t })
    }

    let maxY = -Infinity
    let bottom = 0
    for (let i = 0; i < loop.length; i++) {
      if (loop[i].y > maxY) {
        maxY = loop[i].y
        bottom = i
      }
    }
    const prev = loop[(bottom - 1 + loop.length) % loop.length]
    const next = loop[(bottom + 1) % loop.length]
    const flowSign = next.x - prev.x < 0 ? 1 : -1
    return { loop, spacing, count, flowSign }
  }

  function updateBelt() {
    belt = beltLoop()
    if (belt && hull) {
      const period = belt.spacing * belt.count
      trackPhase += belt.flowSign * hull.velocity.x * 16 * (1 / 60)
      trackPhase = ((trackPhase % period) + period) % period
    }
  }

  function fire() {
    if (!alive) return null
    const dir = hull.angle
    const ox = 120
    const oy = -18
    return {
      dir,
      muzzle: {
        x: hull.position.x + Math.cos(dir) * ox - Math.sin(dir) * oy,
        y: hull.position.y + Math.sin(dir) * ox + Math.cos(dir) * oy,
      },
    }
  }

  return {
    spawn,
    reset,
    step,
    updateBelt,
    fire,
    input,
    get hull() {
      return hull
    },
    get wheels() {
      return wheels
    },
    get idlers() {
      return idlerDiscs()
    },
    get wheelPositions() {
      return wheelPositions()
    },
    get alive() {
      return alive
    },
    get belt() {
      return belt
    },
    get trackPhase() {
      return trackPhase
    },
  }
}

export { WHEELS_LOCAL, IDLERS_LOCAL }
