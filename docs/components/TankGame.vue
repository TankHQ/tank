<script setup>
import { ref, onMounted, onBeforeUnmount } from "vue"
import { createTerrain } from "./tank/terrain.js"
import { createTank, CFG } from "./tank/tank.js"
import { createRenderer } from "./tank/renderer.js"

const wrapRef = ref(null)
const canvasRef = ref(null)
const active = ref(false)
const started = ref(false)
const over = ref(false)

const hud = ref({ speed: 0, dist: 0, throttle: 0, airtime: 0, fps: 0, wheels: [] })

let cleanup = () => {}

onMounted(async () => {
  const mod = await import("matter-js")
  const Matter = mod.default || mod
  const { Bodies, Body, Composite, Engine } = Matter

  const wrap = wrapRef.value
  const canvas = canvasRef.value
  const base = (import.meta.env && import.meta.env.BASE_URL) || "/"

  const CAT = { ground: 0x0001, hull: 0x0002, wheel: 0x0004, shell: 0x0008 }

  const engine = Engine.create()
  engine.gravity.y = CFG.gravity
  const world = engine.world

  const terrain = createTerrain({ Bodies, Composite, world, CAT })
  const renderer = createRenderer(canvas, base)

  const particles = []
  const shells = []

  const tank = createTank({
    Matter,
    engine,
    world,
    groundY: terrain.groundY,
  })

  function fireShell() {
    const shot = tank.fire()
    if (!shot) return
    const { dir, muzzle } = shot
    const speed = 30
    const body = Bodies.circle(muzzle.x, muzzle.y, 5, {
      density: 0.004,
      friction: 0.3,
      restitution: 0.35,
      frictionAir: 0.0015,
      label: "shell",
      collisionFilter: { category: CAT.shell, mask: CAT.ground },
    })
    Body.setVelocity(body, { x: Math.cos(dir) * speed, y: Math.sin(dir) * speed })
    Composite.add(world, body)
    shells.push({ body, life: 2.6 })
    shake = Math.min(16, shake + 11)
    for (let i = 0; i < 26; i++) {
      const a = dir + (Math.random() - 0.5) * 0.9
      const sp = 3 + Math.random() * 9
      particles.push({
        x: muzzle.x, y: muzzle.y,
        vx: Math.cos(a) * sp, vy: Math.sin(a) * sp,
        life: 0.35 + Math.random() * 0.3, max: 0.65,
        size: 2 + Math.random() * 4,
        color: Math.random() < 0.5 ? "#ffd27a" : "#f38b3a",
        grav: 0.02,
      })
    }
  }

  const cam = { x: 0, y: 0 }
  let shake = 0
  let dist = 0
  let airtime = 0
  let dustTimer = 0

  function emitParticle(p) {
    particles.push(p)
  }

  function stepPhysics(dt) {
    terrain.update(cam.x)
    tank.step(dt)

    if (tank.hull && Math.abs(tank.hull.velocity.x) > 0.2) {
      dustTimer++
      if (dustTimer > 6) {
        dustTimer = 0
        const h = tank.hull
        emitParticle({
          x: h.position.x + Math.cos(h.angle) * -112,
          y: h.position.y + Math.sin(h.angle) * -112 - 28,
          vx: -tank.hull.velocity.x * 0.25,
          vy: -0.6 - Math.random(),
          life: 0.7, max: 1.2, size: 3 + Math.random() * 4,
          color: "#6f6a5a", grav: -0.01,
        })
      }
    }

    for (let i = particles.length - 1; i >= 0; i--) {
      const p = particles[i]
      p.life -= dt / 1000
      p.x += p.vx * 60 * (dt / 1000)
      p.y += p.vy * 60 * (dt / 1000)
      p.vy += p.grav * 60 * (dt / 1000) * 60
      if (p.life <= 0) particles.splice(i, 1)
    }

    for (let i = shells.length - 1; i >= 0; i--) {
      const s = shells[i]
      s.life -= dt / 1000
      if (s.life <= 0) {
        for (let k = 0; k < 14; k++) {
          emitParticle({
            x: s.body.position.x, y: s.body.position.y,
            vx: (Math.random() - 0.5) * 6, vy: -Math.random() * 5,
            life: 0.4 + Math.random() * 0.4, max: 0.8, size: 2 + Math.random() * 4,
            color: Math.random() < 0.6 ? "#5a5140" : "#a58c5c", grav: 0.03,
          })
        }
        Composite.remove(world, s.body, true)
        shells.splice(i, 1)
      }
    }
  }

  function updateCamera(dt) {
    const W = renderer.width
    const H = renderer.height
    if (tank.alive && tank.hull) {
      cam.x += (tank.hull.position.x - W * 0.42 - cam.x) * 0.12
      cam.y += (tank.hull.position.y - H * 0.64 - cam.y) * 0.08
    } else {
      cam.x += (-W * 0.42 - cam.x) * 0.05
    }
    shake *= 0.88
    if (shake < 0.1) shake = 0
  }

  let running = false
  let raf = 0
  let last = 0
  let acc = 0
  let fpsAcc = 0
  let fpsFrames = 0
  let fps = 0
  let hudTick = 0
  const FIXED = 1000 / 60

  function frame(now) {
    if (!running) return
    raf = requestAnimationFrame(frame)
    if (!last) last = now
    let dt = now - last
    last = now
    if (dt > 100) dt = 100
    acc += dt

    let steps = 0
    while (acc >= FIXED && steps < 5) {
      stepPhysics(FIXED)
      acc -= FIXED
      steps++
    }
    if (steps === 5) acc = 0

    if (tank.alive && tank.hull) {
      tank.updateBelt()
      dist = Math.max(dist, tank.hull.position.x)
      airtime = tank.hull.position.y < terrain.groundY(tank.hull.position.x) - 130 ? airtime + dt / 1000 : 0
      if (Math.abs(tank.hull.angle) > 2.1 || tank.hull.position.y > terrain.groundY(tank.hull.position.x) + 400) {
        tank.spawn(0)
        dist = 0
      }
    }

    updateCamera(dt)
    renderer.render({ cam, shake, tank, particles, shells, groundY: terrain.groundY })

    fpsAcc += dt
    fpsFrames++
    if (fpsAcc > 500) {
      fps = Math.round(fpsFrames / (fpsAcc / 1000))
      fpsAcc = 0
      fpsFrames = 0
    }

    hudTick += dt
    if (hudTick > 60) {
      hudTick = 0
      hud.value = {
        speed: tank.alive && tank.hull ? Math.round(Math.abs(tank.hull.velocity.x) * 12) : 0,
        dist: Math.round(dist / 10),
        throttle: tank.input.w ? 1 : tank.input.s ? -1 : 0,
        airtime,
        fps,
        wheels: tank.wheels.map((w) => w.compression),
      }
    }
  }

  function start() {
    if (running) return
    running = true
    last = 0
    raf = requestAnimationFrame(frame)
  }

  function keyDown(e) {
    const tag = (e.target && e.target.tagName) || ""
    if (tag === "INPUT" || tag === "TEXTAREA") return
    const k = e.code
    if (["KeyW", "KeyS", "ArrowUp", "ArrowDown", "Space"].includes(k) && (active.value || over.value)) {
      e.preventDefault()
    }
    if (k === "KeyW" || k === "ArrowUp") {
      tank.input.w = true
      started.value = true
    }
    if (k === "KeyS" || k === "ArrowDown") {
      tank.input.s = true
      started.value = true
    }
    if (k === "KeyR") {
      tank.spawn(0)
      dist = 0
    }
    if (k === "Space" && (active.value || over.value)) fireShell()
  }
  function keyUp(e) {
    const k = e.code
    if (k === "KeyW" || k === "ArrowUp") tank.input.w = false
    if (k === "KeyS" || k === "ArrowDown") tank.input.s = false
  }

  const onPointerDown = () => fireShell()
  const onEnter = () => (over.value = true)
  const onLeave = () => (over.value = false)
  const onFocus = () => {
    active.value = true
    started.value = true
  }
  const onBlur = () => (active.value = false)
  const onResize = () => renderer.resize(wrap.getBoundingClientRect())

  window.addEventListener("keydown", keyDown, { passive: false })
  window.addEventListener("keyup", keyUp)
  window.addEventListener("resize", onResize)
  canvas.addEventListener("pointerdown", onPointerDown)
  wrap.addEventListener("pointerenter", onEnter)
  wrap.addEventListener("pointerleave", onLeave)
  wrap.addEventListener("focus", onFocus)
  wrap.addEventListener("blur", onBlur)

  renderer.resize(wrap.getBoundingClientRect())
  tank.spawn(0)
  tank.updateBelt()
  terrain.update(0)
  start()

  cleanup = () => {
    running = false
    cancelAnimationFrame(raf)
    window.removeEventListener("keydown", keyDown)
    window.removeEventListener("keyup", keyUp)
    window.removeEventListener("resize", onResize)
    canvas.removeEventListener("pointerdown", onPointerDown)
    wrap.removeEventListener("pointerenter", onEnter)
    wrap.removeEventListener("pointerleave", onLeave)
    wrap.removeEventListener("focus", onFocus)
    wrap.removeEventListener("blur", onBlur)
    Engine.clear(engine)
    Composite.clear(world, false)
  }
})

onBeforeUnmount(() => cleanup())
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
          <span class="v">{{ String(hud.dist).padStart(5, "0") }} <i>m</i></span>
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
