import { SVG_MM } from './silhouette.js'
import { RUNNING_GEAR } from './config.js'

const SV = SVG_MM
const svgHullMinX = Math.min(...SV.hull.map((p) => p[0]))
const svgHullMaxX = Math.max(...SV.hull.map((p) => p[0]))
const svgHullMinY = Math.min(...SV.hull.map((p) => p[1])) // Hull deck Y in SVG (mm, y down)
const svgHullMaxY = Math.max(...SV.hull.map((p) => p[1])) // Hull skirt Y in SVG (mm, y down)

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

function buildRunningGear(rg) {
    const L = rg.hullPixelLength
    const k = L / (svgHullMaxX - svgHullMinX) // Scale factor (px/mm)
    const raw = (p) => ({ x: (p[0] - svgHullMinX) * k, y: (p[1] - svgHullMinY) * k })

    const hullPoints = SV.hull.map(raw)
    const hullPhysics = SV.hullPhysics.map(raw)
    const turret = SV.turret.map(raw)
    const camo = SV.camo.map((field, i) => ({
        fill: SV.camoFills[i % SV.camoFills.length],
        points: field.map(raw),
    }))

    const wheelRadius = rg.wheelRadiusRatio * L
    const wheelSpacing = rg.wheelSpacingRatio * L
    const idlerRadius = rg.idlerRadiusRatio * L
    const hubDrop = rg.hubDropRatio * L
    const idlerRise = rg.idlerRiseRatio * L
    const restLength = rg.restLengthRatio * L

    const skirtY = (svgHullMaxY - svgHullMinY) * k
    const hubCentreY = skirtY + hubDrop

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

    const idlers = [
        { x: hullCentreX + rg.frontIdlerXRatio * L, y: hubCentreY - idlerRise, radius: idlerRadius },
        { x: hullCentreX + rg.rearIdlerXRatio * L, y: hubCentreY - idlerRise, radius: idlerRadius },
    ]

    // Re-center all local geometry on the physics hull centroid (px) to match Matter's body origin.
    const c = polygonCentroid(hullPhysics)
    const rel = (p) => ({ x: p.x - c.x, y: p.y - c.y })
    const relArr = (arr) => arr.map(rel)

    const hull = relArr(hullPoints)
    const physics = relArr(hullPhysics)
    for (const w of wheels) { w.x -= c.x; w.y -= c.y }
    for (const i of idlers) { i.x -= c.x; i.y -= c.y }

    const turretPoints = relArr(turret)
    const turretRoof = Math.min(...turretPoints.map((p) => p.y))

    const anchor = rel(raw(SV.gunAnchor))
    const noseX = Math.max(...hull.map((p) => p.x))
    const gun = {
        anchorX: anchor.x,
        anchorY: anchor.y,
        muzzleX: noseX + rg.barrelLengthRatio * L,
        muzzleY: anchor.y,
    }

    const camoFields = camo.map((field) => ({ fill: field.fill, points: relArr(field.points) }))

    const wheelTopY = hubCentreY - wheelRadius
    const sponsorBottomY = wheelTopY + 2 * wheelRadius * rg.sponsonDropRatio
    const belly = hullPoints
        .filter((p) => p.y >= skirtY - 1)
        .sort((a, b) => a.x - b.x)
    const bellyRear = belly[0]
    const bellyFront = belly[belly.length - 1]
    const taper = (bellyFront.x - bellyRear.x) * rg.sponsonTaperRatio
    const sponsonRaw = [
        { x: bellyRear.x, y: bellyRear.y - 3 },
        { x: bellyFront.x, y: bellyFront.y - 3 },
        { x: bellyFront.x - taper, y: sponsorBottomY },
        { x: bellyRear.x + taper, y: sponsorBottomY },
    ]
    const sponson = relArr(sponsonRaw)

    const x0 = Math.min(...hull.map((p) => p.x))
    const x1 = Math.max(...hull.map((p) => p.x))
    const yTop = Math.min(...hull.map((p) => p.y))
    const yBot = Math.max(...hull.map((p) => p.y))
    const W = x1 - x0
    const D = yBot - yTop
    const decor = [
        { kind: 'bin', x: x0 + W * 0.18, y: yTop + D * 0.18, w: W * 0.13, h: D * 0.34 },
        { kind: 'bin', x: x0 + W * 0.52, y: yTop + D * 0.18, w: W * 0.14, h: D * 0.34 },
        { kind: 'hatch', x: x0 + W * 0.35, y: yTop + D * 0.3, r: D * 0.13 },
        { kind: 'light', x: x1 - W * 0.035, y: yTop + D * 0.42, r: D * 0.07 },
        { kind: 'hook', x: x1 - W * 0.02, y: yBot - D * 0.2, w: W * 0.03, h: D * 0.16 },
        { kind: 'hook', x: x1 - W * 0.08, y: yBot - D * 0.2, w: W * 0.03, h: D * 0.16 },
    ]

    return {
        wheels,
        idlers,
        hullPoints: hull,
        hullPhysics: physics,
        turretPoints,
        turretRoof,
        camo: camoFields,
        sponson,
        decor,
        gun,
    }
}

const GEAR = buildRunningGear(RUNNING_GEAR)

export const WHEEL_MOUNTS = GEAR.wheels // Road-wheel mounts in hull-local frame (px)
export const IDLER_MOUNTS = GEAR.idlers // Fixed idler mounts in hull-local frame (px)
export const HULL_POINTS = GEAR.hullPhysics // Physics hull polygon in hull-local frame (px)
export const HULL_ART = GEAR.hullPoints // Drawable hull polygon in hull-local frame (px)

export const TURRET = {
    points: GEAR.turretPoints,
    roof: GEAR.turretRoof,
}

export const GUN = {
    anchorX: GEAR.gun.anchorX,
    anchorY: GEAR.gun.anchorY,
    muzzleX: GEAR.gun.muzzleX,
    muzzleY: GEAR.gun.muzzleY,
    radius: RUNNING_GEAR.barrelRadiusRatio * RUNNING_GEAR.hullPixelLength,
}

export const CAMO = GEAR.camo
export const SPONSON = GEAR.sponson
export const DECOR = GEAR.decor

const hullMinX = Math.min(...GEAR.hullPoints.map((p) => p.x))
const hullMaxX = Math.max(...GEAR.hullPoints.map((p) => p.x))
const hullMinY = Math.min(...GEAR.hullPoints.map((p) => p.y))
const hullMaxY = Math.max(...GEAR.hullPoints.map((p) => p.y))

export const BODY = {
    minX: hullMinX,
    deckY: hullMinY,
    skirtY: hullMaxY,
    length: hullMaxX - hullMinX,
    depth: hullMaxY - hullMinY,
}

export const SUSPENSION = {
    restLength: RUNNING_GEAR.restLengthRatio * RUNNING_GEAR.hullPixelLength, // Unloaded spring length (px)
    fullyCompressedLength: RUNNING_GEAR.fullyCompressedRatio * RUNNING_GEAR.hullPixelLength, // Min spring length (px)
    fullyExtendedLength: RUNNING_GEAR.fullyExtendedRatio * RUNNING_GEAR.hullPixelLength, // Max spring length (px)
}

export const TRACK = {
    padLength: RUNNING_GEAR.trackPadLength,
    clearance: RUNNING_GEAR.trackClearance,
    scrollRate: RUNNING_GEAR.trackScrollRate,
    minPads: RUNNING_GEAR.trackMinPads,
}
