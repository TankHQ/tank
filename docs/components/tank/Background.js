import { valueNoise } from './util.js'


export class Background {
    constructor(cfg) {
        this.cfg = cfg
    }

    _ridge(x, layer) {
        const { octaves, persistence, lacunarity } = this.cfg
        let sum = 0
        let norm = 0
        let amp = 1
        let freq = layer.frequency
        for (let o = 0; o < octaves; o++) {
            const n = valueNoise(x * freq + layer.seed + o * 37.13)
            const ridged = 1 - Math.abs(n * 2 - 1)
            sum += ridged * amp
            norm += amp
            amp *= persistence
            freq *= lacunarity
        }
        return sum / norm
    }

    _drawSky(ctx, W, H) {
        const { sky } = this.cfg
        const g = ctx.createLinearGradient(0, 0, 0, H)
        for (let i = 0; i < sky.length; i++) g.addColorStop(i / (sky.length - 1), sky[i])
        ctx.fillStyle = g
        ctx.fillRect(0, 0, W, H)
    }

    _drawSun(ctx, W, H) {
        const { sun } = this.cfg
        const x = W * sun.x
        const y = H * sun.y
        const r = sun.radius * H

        const glow = ctx.createRadialGradient(x, y, 0, x, y, r * 6)
        glow.addColorStop(0, sun.glow)
        glow.addColorStop(1, 'rgba(255,196,120,0)')
        ctx.fillStyle = glow
        ctx.fillRect(0, 0, W, H)

        ctx.beginPath()
        ctx.arc(x, y, r, 0, Math.PI * 2)
        ctx.fillStyle = sun.color
        ctx.fill()
    }

    _drawLayer(ctx, W, H, cameraX, layer) {
        const step = this.cfg.step
        const camWorld = cameraX * layer.parallax
        const screenX = (worldX) => worldX - camWorld
        // Vertices sit at fixed world positions, so the profile only translates.
        const i0 = Math.floor(camWorld / step) - 1
        const i1 = Math.ceil((camWorld + W) / step) + 1

        const baseY = layer.baseY * H
        const amp = layer.amplitude * H
        const sharp = this.cfg.sharpness

        ctx.beginPath()
        ctx.moveTo(screenX(i0 * step), H + 4)
        for (let i = i0; i <= i1; i++) {
            const worldX = i * step
            const y = baseY - amp * Math.pow(this._ridge(worldX, layer), sharp)
            ctx.lineTo(screenX(worldX), y)
        }
        ctx.lineTo(screenX(i1 * step), H + 4)
        ctx.closePath()

        // Gradient spans the visible band (peak -> foot), so the light foot tone is
        // not wasted below the ground line.
        const g = ctx.createLinearGradient(0, baseY - amp, 0, baseY)
        g.addColorStop(0, layer.peak)
        g.addColorStop(1, layer.base)
        ctx.fillStyle = g
        ctx.fill()
    }

    draw(ctx, cameraX, W, H) {
        this._drawSky(ctx, W, H)
        this._drawSun(ctx, W, H)
        for (const layer of this.cfg.layers) this._drawLayer(ctx, W, H, cameraX, layer)
    }
}
