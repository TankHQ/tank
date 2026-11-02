import { onBeforeUnmount, ref } from "vue"

export function useDrag({ onStart, onMove, onEnd, threshold = 6 } = {}) {
    const dragging = ref(false)
    let sx = 0
    let sy = 0
    let axis = null
    let samples = []

    const stop = () => {
        dragging.value = false
        window.removeEventListener("pointermove", move)
        window.removeEventListener("pointerup", up)
        window.removeEventListener("pointercancel", up)
    }

    const move = (e) => {
        if (!dragging.value) return
        const dx = e.clientX - sx
        const dy = e.clientY - sy
        if (!axis) {
            if (Math.hypot(dx, dy) < threshold) return
            axis = Math.abs(dx) >= Math.abs(dy) ? "x" : "y"
            if (axis === "y") return stop()
        }
        if (e.cancelable) e.preventDefault()
        samples.push({ x: e.clientX, y: e.clientY, t: performance.now() })
        if (samples.length > 6) samples.shift()
        onMove?.({ dx, dy, e })
    }

    const up = (e) => {
        if (!dragging.value) return
        const moved = axis === "x"
        stop()
        const a = samples[0]
        const b = samples[samples.length - 1]
        const dt = b.t - a.t || 16
        onEnd?.({
            dx: e.clientX - sx,
            dy: e.clientY - sy,
            vx: (b.x - a.x) / dt,
            vy: (b.y - a.y) / dt,
            moved,
            e,
        })
    }

    const down = (e) => {
        if ((e.pointerType === "mouse" && e.button !== 0) || e.target.closest?.("button, label, input")) return
        sx = e.clientX
        sy = e.clientY
        axis = null
        samples = [{ x: sx, y: sy, t: performance.now() }]
        dragging.value = true
        onStart?.(e)
        window.addEventListener("pointermove", move, { passive: false })
        window.addEventListener("pointerup", up)
        window.addEventListener("pointercancel", up)
    }

    onBeforeUnmount(stop)
    return { dragging, down }
}
