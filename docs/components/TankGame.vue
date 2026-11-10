<script setup>
    import { ref, onMounted, onBeforeUnmount } from "vue"
    import { Game } from "./tank/Game.js"

    const wrapRef = ref(null)
    const canvasRef = ref(null)
    const active = ref(false)

    const hud = ref({ speed: 0, distance: 0, throttle: 1, fps: 0, wheels: [] })

    let game = null
    let detach = () => { }
    let unmounted = false

    onMounted(async () => {
        const wrap = wrapRef.value
        const canvas = canvasRef.value
        const hero = document.querySelector(".VPHero")
        if (hero && wrap) hero.prepend(wrap)

        const mod = await import("matter-js")
        if (unmounted) return
        const Matter = mod.default || mod

        const base = (import.meta.env && import.meta.env.BASE_URL) || "/"

        game = new Game({ Matter, canvas, base })
        game.onHud((stats) => { hud.value = stats })
        game.start(wrap.getBoundingClientRect())

        const setFocused = (focused) => {
            active.value = focused
            hero?.classList.toggle("game-focused", focused)
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
            hero?.classList.remove("game-focused")
            window.removeEventListener("keydown", keyDown)
            window.removeEventListener("keyup", keyUp)
            window.removeEventListener("resize", onResize)
            window.removeEventListener("pointerdown", onDocPointerDown)
            canvas.removeEventListener("pointerdown", onPointerDown)
            wrap.removeEventListener("focus", onFocus)
            wrap.removeEventListener("blur", onBlur)
            wrap.remove()
        }
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

        <div class="tank-game__hud">
            <div class="tank-game__panel tank-game__panel--left">
                <div class="tank-game__brand">TANK <span>FIELD SIM</span></div>
                <div class="tank-game__row">
                    <span class="k">SPEED</span>
                    <span class="v">{{ String(hud.speed).padStart(3, "0") }} <i>km/h</i></span>
                </div>
                <div class="tank-game__row">
                    <span class="k">DISTANCE</span>
                    <span class="v">{{ String(hud.distance).padStart(5, "0") }} <i>m</i></span>
                </div>
                <div class="tank-game__row">
                    <span class="k">ENGINE</span>
                    <span class="v">{{ hud.throttle > 0 ? "DRIVE" : hud.throttle < 0 ? "REVERSE" : "IDLE" }}</span>
                </div>
            </div>

            <div class="tank-game__panel tank-game__panel--right">
                <div class="tank-game__susp-label">SUSPENSION TRAVEL</div>
                <div class="tank-game__wheels">
                    <div v-for="(t, i) in hud.wheels" :key="i" class="tank-game__wheel">
                        <div class="bar">
                            <div class="fill" :style="{ height: (t * 100).toFixed(0) + '%' }"></div>
                        </div>
                    </div>
                </div>
                <div class="tank-game__fps">{{ hud.fps }} FPS</div>
            </div>
        </div>

        <div class="tank-game__hint">
            <b>W</b> / <b>S</b> drive &nbsp;·&nbsp; <b>Click</b> fire &nbsp;·&nbsp; <b>R</b> reset
        </div>
    </section>
</template>

<style scoped>
    .tank-game {
        position: absolute;
        inset: 0;
        z-index: 0;
        width: 100%;
        height: 100%;
        overflow: hidden;
        outline: none;
        cursor: pointer;
        user-select: none;
        background: #241c1a;
    }

    .tank-game.is-active {
        cursor: crosshair;
    }

    .tank-game__canvas {
        display: block;
        width: 100%;
        height: 100%;
    }

    .tank-game__hud {
        position: absolute;
        inset: 0;
        pointer-events: none;
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        padding: 14px 18px;
        font-family: "Courier New", ui-monospace, monospace;
        color: #e8ddc4;
        text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
        opacity: 0;
        transition: opacity 0.4s ease;
    }

    .tank-game.is-active .tank-game__hud,
    .tank-game.is-active .tank-game__hint {
        opacity: 1;
    }

    .tank-game__panel {
        background: rgba(12, 10, 8, 0.46);
        border: 1px solid rgba(191, 177, 145, 0.28);
        border-radius: 4px;
        padding: 8px 12px;
        backdrop-filter: blur(3px);
        min-width: 150px;
    }

    .tank-game__brand {
        font-size: 13px;
        letter-spacing: 3px;
        font-weight: 700;
        color: #d7c7a6;
        border-bottom: 1px solid rgba(191, 177, 145, 0.25);
        padding-bottom: 5px;
        margin-bottom: 6px;
    }

    .tank-game__brand span {
        font-size: 9px;
        letter-spacing: 1px;
        color: #8f8155;
    }

    .tank-game__row {
        display: flex;
        justify-content: space-between;
        gap: 18px;
        font-size: 11px;
        letter-spacing: 1px;
        line-height: 1.7;
    }

    .tank-game__row .k {
        color: #9c937c;
    }

    .tank-game__row .v {
        color: #f0e6cd;
        font-weight: 700;
    }

    .tank-game__row .v i {
        font-style: normal;
        font-size: 9px;
        color: #9c937c;
    }

    .tank-game__panel--right {
        text-align: right;
        min-width: 190px;
    }

    .tank-game__susp-label {
        font-size: 9px;
        letter-spacing: 2px;
        color: #9c937c;
        margin-bottom: 6px;
    }

    .tank-game__wheels {
        display: flex;
        gap: 6px;
        justify-content: flex-end;
        align-items: flex-end;
        height: 42px;
    }

    .tank-game__wheel .bar {
        width: 12px;
        height: 40px;
        background: rgba(0, 0, 0, 0.5);
        border: 1px solid rgba(191, 177, 145, 0.3);
        border-radius: 2px;
        display: flex;
        align-items: flex-end;
        overflow: hidden;
    }

    .tank-game__wheel .fill {
        width: 100%;
        background: linear-gradient(to top, #7dae5a, #d8c14a, #d87a3a);
        transition: height 60ms linear;
    }

    .tank-game__fps {
        margin-top: 6px;
        font-size: 9px;
        letter-spacing: 1px;
        color: #9c937c;
    }

    .tank-game__hint {
        position: absolute;
        bottom: 12px;
        left: 50%;
        transform: translateX(-50%);
        font-family: "Courier New", ui-monospace, monospace;
        font-size: 11px;
        letter-spacing: 1px;
        color: #e8ddc4;
        background: rgba(12, 10, 8, 0.5);
        border: 1px solid rgba(191, 177, 145, 0.25);
        border-radius: 999px;
        padding: 5px 14px;
        pointer-events: none;
        text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
        opacity: 0;
        transition: opacity 0.4s ease;
    }

    .tank-game__hint b {
        color: #d8c14a;
    }
</style>
