import { CONFIG } from './config.js'
import { SUSPENSION } from './geometry.js'
import { clamp, mix, toWorld } from './util.js'

const DT2 = (1000 / 60) * (1000 / 60) // Squared 60 Hz step (ms^2) for Matter force integration

export class SuspensionWheel {
    constructor(mount) {
        this.mountLocal = { x: mount.x, y: mount.y } // Hull-local mount coordinates (px)
        this.radius = mount.radius // Wheel radius (px)

        this.currentLength = SUSPENSION.restLength // Current spring extension (px)
        this.compression = 0 // Normalized compression (0..1)
        this.contact = false
        this.contactX = 0
        this.contactY = 0
        this.normalForce = 0
        this.spinAngle = 0 // Cumulative wheel rotation (rad)

        this.previousLength = this.currentLength
        this.previousSpin = 0
    }

    mountPoint(pose) {
        return toWorld(pose.x, pose.y, pose.angle, this.mountLocal.x, this.mountLocal.y)
    }

    centrePoint(pose, extension = this.currentLength) {
        return toWorld(pose.x, pose.y, pose.angle, this.mountLocal.x, this.mountLocal.y + extension)
    }

    // Spring extension (px) required to touch terrain directly below the mount.
    groundContactLength(pose, groundY) {
        const mount = this.mountPoint(pose)
        return groundY(mount.x) - mount.y - this.radius
    }

    capturePrevious() {
        this.previousLength = this.currentLength
        this.previousSpin = this.spinAngle
    }

    // Update spring extension (px); compression is instant, extension capped by maxExtensionThisStep (px).
    advance(pose, groundY, maxExtensionThisStep) {
        const target = this.groundContactLength(pose, groundY)
        const min = SUSPENSION.fullyCompressedLength
        const max = SUSPENSION.fullyExtendedLength

        let contact = false
        if (target < min) {
            this.currentLength = min
            contact = true
        } else if (target > max) {
            this.currentLength = Math.min(max, this.currentLength + maxExtensionThisStep)
        } else if (target <= this.currentLength) {
            this.currentLength = target
            contact = true
        } else {
            this.currentLength = Math.min(target, this.currentLength + maxExtensionThisStep)
            contact = this.currentLength >= target - 0.001
        }

        this._recordContact(pose, contact)
    }

    // Clamp wheel above terrain after hard hull position correction.
    settle(pose, groundY) {
        const target = this.groundContactLength(pose, groundY)
        const min = SUSPENSION.fullyCompressedLength
        if (target < this.currentLength) {
            this.currentLength = Math.max(min, target)
            this._recordContact(pose, true)
        } else if (!this.contact) {
            this._recordContact(pose, false)
        }
    }

    _recordContact(pose, contact) {
        const max = SUSPENSION.restLength - SUSPENSION.fullyCompressedLength
        this.compression = clamp((SUSPENSION.restLength - this.currentLength) / max, 0, 1)
        this.contact = contact
        const mount = this.mountPoint(pose)
        this.contactX = mount.x
        this.contactY = mount.y + this.currentLength
        this.normalForce = 0
    }

    // Compute upward normal force; staticLoad in force units, vDown in px/frame, massPerWheel in mass units.
    suspensionForce(staticLoad, vDown, massPerWheel) {
        const travel = SUSPENSION.restLength - SUSPENSION.fullyCompressedLength
        const u = clamp((SUSPENSION.restLength - this.currentLength) / travel, 0, 1.6)
        const spring = staticLoad * (CONFIG.springLinearStiffness * u + CONFIG.springProgressiveStiffness * u * u * u)
        const wanted = CONFIG.shockAbsorberDamping * vDown
        const maxDamper = (massPerWheel * Math.abs(vDown)) / DT2
        const damper = clamp(wanted, -maxDamper, maxDamper)
        return Math.max(0, spring + damper)
    }

    // Advance spinAngle (rad) from dt (ms) and groundSpeed (px/frame).
    spin(dt, groundSpeed) {
        this.spinAngle += (groundSpeed / this.radius) * (dt / 16.666)
    }

    // Interpolated wheel state for alpha in 0..1.
    view(pose, alpha) {
        const length = mix(this.previousLength, this.currentLength, alpha)
        const spin = mix(this.previousSpin, this.spinAngle, alpha)
        const centre = this.centrePoint(pose, length)
        return {
            x: centre.x,
            y: centre.y,
            radius: this.radius,
            spinAngle: spin,
            compression: this.compression,
            contact: this.contact,
        }
    }
}
