import { SVG_MM } from './silhouette.js'
import { RUNNING_GEAR } from './config.js'

// Everything here is *derived* from the raw settings in `config.js` and the
// reference outline in `silhouette.js`. Nothing in this file is tunable: change
// the settings and these values follow. The renderer, the physics and the
// ballistics all read the same derived geometry, so the art and the simulation
// can never disagree.

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

// Turn the running-gear ratios into concrete pixel positions for the hull, the
// road wheels, the idlers, the cannon and the camouflage, all in the centred,
// body-local frame Matter uses.
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
        gun,
    }
}

const GEAR = buildRunningGear(RUNNING_GEAR)

// Road wheels: mount position relative to the hull centre, and radius.
export const WHEEL_MOUNTS = GEAR.wheels

// Idler wheels ("top wheels"): fixed to the chassis (not sprung).
export const IDLER_MOUNTS = GEAR.idlers

// Chassis outline for the physics body, in body-local coordinates (y down).
export const HULL_POINTS = GEAR.hullPhysics

// The drawable hull outline (higher detail than the physics body).
export const HULL_ART = GEAR.hullPoints

// The turret outline (visual overlay on the hull), plus its top edge.
export const TURRET = {
    points: GEAR.turretPoints,
    roof: GEAR.turretRoof,
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

// Bounding box of the drawable hull, for the renderer's art.
const hullMinX = Math.min(...GEAR.hullPoints.map((p) => p.x))
const hullMaxX = Math.max(...GEAR.hullPoints.map((p) => p.x))
const hullMinY = Math.min(...GEAR.hullPoints.map((p) => p.y)) // deck line
const hullMaxY = Math.max(...GEAR.hullPoints.map((p) => p.y)) // skirt line

export const BODY = {
    minX: hullMinX,
    deckY: hullMinY,
    skirtY: hullMaxY,
    length: hullMaxX - hullMinX,
    depth: hullMaxY - hullMinY,
}

// Suspension geometry, in pixels, derived from the running gear so the mounts
// and wheel centres always agree.
export const SUSPENSION = {
    restLength: RUNNING_GEAR.restLengthRatio * RUNNING_GEAR.hullPixelLength,
    fullyCompressedLength: RUNNING_GEAR.fullyCompressedRatio * RUNNING_GEAR.hullPixelLength,
    fullyExtendedLength: RUNNING_GEAR.fullyExtendedRatio * RUNNING_GEAR.hullPixelLength,
}

// Track construction, wrapping whatever discs the running gear produces.
export const TRACK = {
    padLength: RUNNING_GEAR.trackPadLength,
    clearance: RUNNING_GEAR.trackClearance,
    scrollRate: RUNNING_GEAR.trackScrollRate,
    minPads: RUNNING_GEAR.trackMinPads,
}

// World scale for the HUD: pixels per real-world metre.
export const PIXELS_PER_METRE = RUNNING_GEAR.hullPixelLength / RUNNING_GEAR.hullLengthMetres
