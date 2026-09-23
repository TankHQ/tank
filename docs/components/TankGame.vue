<script setup>
import { ref, onMounted, onBeforeUnmount } from "vue"
import { Game } from "./tank/Game.js"

const wrapRef = ref(null)
const canvasRef = ref(null)
const active = ref(false)
const started = ref(false)
const over = ref(false)

const hud = ref({ speed: 0, distance: 0, throttle: 0, airtime: 0, fps: 0, wheels: [] })

let game = null
let detach = () => {}

onMounted(async () => {
  const mod = await import("planck")
  const planck = mod.default || mod

  const wrap = wrapRef.value
  const canvas = canvasRef.value
  const base = (import.meta.env && import.meta.env.BASE_URL) || "/"

  game = new Game({ planck, canvas, base })
  game.onHud((stats) => { hud.value = stats })
  game.start(wrap.getBoundingClientRect())

  const keyDown = (e) => {
    const tag = (e.target && e.target.tagName) || ""
    if (tag === "INPUT" || tag === "TEXTAREA") return
    const k = e.code
    const interactive = active.value || over.value
    if (["KeyW", "KeyS", "ArrowUp", "ArrowDown", "Space"].includes(k) && interactive) e.preventDefault()

    if (k === "KeyW" || k === "ArrowUp") {
      game.input.w = true
      started.value = true
    }
    if (k === "KeyS" || k === "ArrowDown") {
      game.input.s = true
      started.value = true
    }
    if (k === "KeyR") game.reset()
    if (k === "Space" && interactive) game.fire()
  }
  const keyUp = (e) => {
    if (e.code === "KeyW" || e.code === "ArrowUp") game.input.w = false
    if (e.code === "KeyS" || e.code === "ArrowDown") game.input.s = false
  }
  const onPointerDown = () => game.fire()
  const onEnter = () => (over.value = true)
  const onLeave = () => (over.value = false)
  const onFocus = () => {
    active.value = true
    started.value = true
  }
  const onBlur = () => (active.value = false)
  const onResize = () => game.resize(wrap.getBoundingClientRect())

  window.addEventListener("keydown", keyDown, { passive: false })
  window.addEventListener("keyup", keyUp)
  window.addEventListener("resize", onResize)
  canvas.addEventListener("pointerdown", onPointerDown)
  wrap.addEventListener("pointerenter", onEnter)
  wrap.addEventListener("pointerleave", onLeave)
  wrap.addEventListener("focus", onFocus)
  wrap.addEventListener("blur", onBlur)

  detach = () => {
    window.removeEventListener("keydown", keyDown)
    window.removeEventListener("keyup", keyUp)
    window.removeEventListener("resize", onResize)
    canvas.removeEventListener("pointerdown", onPointerDown)
    wrap.removeEventListener("pointerenter", onEnter)
    wrap.removeEventListener("pointerleave", onLeave)
    wrap.removeEventListener("focus", onFocus)
    wrap.removeEventListener("blur", onBlur)
  }
})

onBeforeUnmount(() => {
  detach()
  if (game) game.stop()
})
</script>

<template>
  <section
    ref="wrapRef"
    class="tank-game"
    tabindex="0"
    @click="started = true"
  >
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

    <transition name="fade">
      <div v-if="!started" class="tank-game__splash">
        <div class="tank-game__splash-inner">
          <div class="tank-game__splash-title">2D SUSPENSION SIMULATION</div>
          <div class="tank-game__splash-sub">
            Rigid-body tracked vehicle · independent coil suspension · chain track
          </div>
          <div class="tank-game__splash-cta">Click to take control</div>
        </div>
      </div>
    </transition>
  </section>
</template>

<style scoped>
.tank-game {
  position: relative;
  width: 100%;
  height: clamp(360px, 52vh, 640px);
  overflow: hidden;
  outline: none;
  cursor: crosshair;
  user-select: none;
  background: #1d2b40;
  border-top: 1px solid var(--vp-c-divider);
  border-bottom: 1px solid var(--vp-c-divider);
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
}
.tank-game__hint b {
  color: #d8c14a;
}

.tank-game__splash {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: radial-gradient(ellipse at center, rgba(10, 8, 6, 0.35), rgba(6, 5, 4, 0.75));
  pointer-events: none;
}
.tank-game__splash-inner {
  text-align: center;
  font-family: "Courier New", ui-monospace, monospace;
  color: #e8ddc4;
}
.tank-game__splash-title {
  font-size: clamp(16px, 2.4vw, 26px);
  letter-spacing: 6px;
  font-weight: 700;
  color: #f0e6cd;
  text-shadow: 0 2px 10px rgba(0, 0, 0, 0.9);
}
.tank-game__splash-sub {
  margin-top: 8px;
  font-size: clamp(10px, 1.1vw, 13px);
  letter-spacing: 1px;
  color: #b3a888;
}
.tank-game__splash-cta {
  margin-top: 16px;
  font-size: 12px;
  letter-spacing: 2px;
  color: #d8c14a;
  animation: pulse 1.6s ease-in-out infinite;
}
@keyframes pulse {
  0%,
  100% {
    opacity: 0.45;
  }
  50% {
    opacity: 1;
  }
}
.fade-leave-active {
  transition: opacity 0.4s ease;
}
.fade-leave-to {
  opacity: 0;
}
</style>
