import { onBeforeUnmount, ref } from "vue"

/**
 * Shared click-and-drag gesture used by the draggable marquee and the card deck.
 * Reports pointer deltas and tells you whether the gesture actually moved, so a
 * tap can still behave as a click.
 */
export function useDrag({ onStart, onMove, onEnd, threshold = 6, mouseOnly = false } = {}) {
    const dragging = ref(false)
    let startX = 0
    let startY = 0
    let moved = false

    const move = (e) => {
        if (!dragging.value) return
        const dx = e.clientX - startX
        const dy = e.clientY - startY
        if (!moved && Math.hypot(dx, dy) > threshold) moved = true
        if (moved) e.preventDefault()
        onMove?.({ dx, dy, e, moved })
    }

    const up = (e) => {
        if (!dragging.value) return
        dragging.value = false
        window.removeEventListener("pointermove", move)
        window.removeEventListener("pointerup", up)
        window.removeEventListener("pointercancel", up)
        onEnd?.({ dx: e.clientX - startX, dy: e.clientY - startY, e, moved })
    }

    const down = (e) => {
        if (mouseOnly && e.pointerType !== "mouse") return
        if (e.pointerType === "mouse" && e.button !== 0) return
        startX = e.clientX
        startY = e.clientY
        moved = false
        dragging.value = true
        onStart?.(e)
        window.addEventListener("pointermove", move, { passive: false })
        window.addEventListener("pointerup", up)
        window.addEventListener("pointercancel", up)
    }

    const suppressClick = (e) => {
        if (moved) {
            e.stopPropagation()
            e.preventDefault()
            moved = false
        }
    }

    onBeforeUnmount(() => {
        window.removeEventListener("pointermove", move)
        window.removeEventListener("pointerup", up)
        window.removeEventListener("pointercancel", up)
    })

    return { dragging, down, suppressClick }
}
