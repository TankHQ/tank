<script setup>
    import { ref, onMounted, onBeforeUnmount } from "vue"

    const wrapRef = ref(null)
    const canvasRef = ref(null)
    const active = ref(false)

    let game = null
    let detach = () => { }
    let unmounted = false

    onMounted(() => {
        const wrap = wrapRef.value
        const canvas = canvasRef.value

        const io = new IntersectionObserver(async (entries) => {
            if (!entries.some((e) => e.isIntersecting)) return
            io.disconnect()

            const { Game } = await import("./tank/Game.js")
            if (unmounted) return

            const base = (import.meta.env && import.meta.env.BASE_URL) || "/"

            game = new Game({ canvas, base })
            game.start(wrap.getBoundingClientRect())

            const setFocused = (focused) => {
                active.value = focused
                if (game && !focused) {
                    game.input.cruise = true
                    game.input.w = false
                    game.input.s = false
                }
            }

            const keyDown = (e) => {
                if (!active.value) return
                const tag = (e.target && e.target.tagName) || ""
                if (tag === "INPUT" || tag === "TEXTAREA") return
                const k = e.code
                if (["KeyW", "KeyS", "ArrowUp", "ArrowDown", "Space"].includes(k)) e.preventDefault()

                if (k === "KeyW" || k === "ArrowUp") {
                    game.input.cruise = false
                    game.input.w = true
                }
                if (k === "KeyS" || k === "ArrowDown") {
                    game.input.cruise = false
                    game.input.s = true
                }
                if (k === "KeyR") game.reset()
                if (k === "Space") game.fire()
                if (k === "Escape") {
                    setFocused(false)
                    wrap.blur()
                }
            }
            const keyUp = (e) => {
                if (!game) return
                if (e.code === "KeyW" || e.code === "ArrowUp") game.input.w = false
                if (e.code === "KeyS" || e.code === "ArrowDown") game.input.s = false
            }
            const onPointerDown = (e) => {
                const wasActive = active.value
                e.preventDefault()
                setFocused(true)
                wrap.focus({ preventScroll: true })
                if (wasActive) game.fire()
            }
            const onDocPointerDown = (e) => {
                if (active.value && !wrap.contains(e.target)) {
                    setFocused(false)
                    wrap.blur()
                }
            }
            const onFocus = () => setFocused(true)
            const onBlur = () => setFocused(false)
            const onResize = () => game.resize(wrap.getBoundingClientRect())
            const ro = new ResizeObserver(onResize)
            ro.observe(wrap)

            window.addEventListener("keydown", keyDown, { passive: false })
            window.addEventListener("keyup", keyUp)
            window.addEventListener("resize", onResize)
            window.addEventListener("pointerdown", onDocPointerDown)
            canvas.addEventListener("pointerdown", onPointerDown)
            wrap.addEventListener("focus", onFocus)
            wrap.addEventListener("blur", onBlur)

            detach = () => {
                ro.disconnect()
                window.removeEventListener("keydown", keyDown)
                window.removeEventListener("keyup", keyUp)
                window.removeEventListener("resize", onResize)
                window.removeEventListener("pointerdown", onDocPointerDown)
                canvas.removeEventListener("pointerdown", onPointerDown)
                wrap.removeEventListener("focus", onFocus)
                wrap.removeEventListener("blur", onBlur)
            }
        })
        io.observe(canvas)
        detach = () => io.disconnect()
    })

    onBeforeUnmount(() => {
        unmounted = true
        detach()
        if (game) game.stop()
    })
</script>

<template>
    <section ref="wrapRef" class="tank-game" :class="{ 'is-active': active }" tabindex="0">
        <canvas ref="canvasRef" class="tank-game__canvas"></canvas>

        <div class="tank-game__hint">
            <b>W</b> / <b>S</b> drive &nbsp;·&nbsp; <b>Click</b> fire &nbsp;·&nbsp; <b>R</b> reset
        </div>
    </section>
</template>

<style scoped>
    .tank-game {
        position: relative;
        margin: 80px var(--vp-offset, calc(50% - 50vw)) 0;
        height: clamp(360px, 52vh, 640px);
        overflow: hidden;
        outline: none;
        cursor: crosshair;
        user-select: none;
    }

    .tank-game__canvas {
        display: block;
        width: 100%;
        height: 100%;
    }

    .tank-game__hint {
        position: absolute;
        bottom: 12px;
        left: 50%;
        transform: translateX(-50%);
        pointer-events: none;
        font-family: "Courier New", ui-monospace, monospace;
        font-size: 11px;
        letter-spacing: 1px;
        color: #e8ddc4;
        text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
        background: rgba(12, 10, 8, 0.5);
        border: 1px solid rgba(191, 177, 145, 0.25);
        border-radius: 999px;
        padding: 5px 14px;
    }

    .tank-game__hint b {
        color: #d8c14a;
    }
</style>
