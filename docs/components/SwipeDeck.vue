<script setup>
    import { onBeforeUnmount, onMounted, ref } from "vue"

    // A Tinder-style deck of cards, fed from a JSON file (like the jokes). Each
    // entry carries a Rust snippet (the card) and its generated SQL per database
    // (the fixed window). Both sides are syntax-highlighted at runtime with the
    // same Shiki themes VitePress uses, and the generated markup reuses VitePress's
    // own `vp-code` / `vp-code-group` classes so no extra styling is needed.
    //
    // Throwing a card reuses the same Matter.js physics engine the tank game uses
    // to carry its momentum until it fades out.

    const props = defineProps({
        src: { type: String, default: "" },
    })

    const VISIBLE = 5
    const THRESHOLD = 120 // px of horizontal travel required to throw
    const MAX_SPEED = 42 // px per frame, keeps the throw readable
    const MAX_SPIN = 0.06 // rad per frame

    let dragFade = 420
    let flyFade = 760

    const stage = ref(null)
    const outputWindow = ref(null)
    const dragging = ref(false)
    const loading = ref(true)
    const failed = ref(false)

    let Matter = null
    let engine = null
    let body = null

    let cards = []
    let cardOutputs = new Map()
    let activeEl = null
    let flying = false
    let raf = null
    let lastT = 0
    let restCenter = { x: 0, y: 0 }
    let throwOrigin = { x: 0, y: 0 }
    let drag = { dx: 0, dy: 0 }
    let samples = []
    let pid = null
    let sx = 0
    let sy = 0
    let axis = null
    let moved = false
    let settleTimer = null

    const clamp = (v, a, b) => Math.min(b, Math.max(a, v))
    const esc = (s) =>
        String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]))

    // A snippet is stored as an array of lines (JSON has no multiline strings), so
    // joining them is our `indoc!`: the file's own indentation is structural and the
    // content is explicit. A plain string is still accepted and dedented like indoc.
    const code = (v) => {
        if (Array.isArray(v)) return v.join("\n")
        const lines = String(v).split("\n")
        if (lines.length > 1 && lines[0].trim() === "") lines.shift()
        if (lines.length > 1 && lines[lines.length - 1].trim() === "") lines.pop()
        const indent = lines
            .filter((l) => l.trim())
            .reduce((m, l) => Math.min(m, l.match(/^\s*/)[0].length), Infinity)
        return lines.map((l) => l.slice(indent)).join("\n")
    }

    const slot = (i) =>
        `translate3d(${-i * 7}px, ${-i * 13}px, 0) rotate(${-i * 1.6}deg) scale(${1 - i * 0.035})`

    const render = () => {
        cards.forEach((el, i) => {
            if (el === activeEl) return
            el.style.zIndex = String(1000 - i)
            if (i >= VISIBLE) {
                el.style.opacity = "0"
                el.style.transform = slot(VISIBLE)
                return
            }
            el.style.opacity = "1"
            el.style.transform = slot(i)
        })
    }

    // When the top card leaves, the rest glide up one slot and settle vertical.
    const animateSettle = () => {
        cards.forEach((el) => {
            if (el === activeEl) return
            el.style.transition = "transform 0.4s cubic-bezier(.2,.8,.25,1)"
        })
        render()
        clearTimeout(settleTimer)
        settleTimer = setTimeout(() => {
            cards.forEach((el) => {
                if (el !== activeEl) el.style.transition = ""
            })
        }, 420)
    }

    const showOutput = (card) => {
        const active = cardOutputs.get(card)
        for (const out of cardOutputs.values()) {
            out.classList.toggle("is-active", out === active)
        }
    }

    const measure = () => {
        if (!stage.value) return
        const r = stage.value.getBoundingClientRect()
        restCenter = { x: r.width / 2, y: r.height / 2 }
        dragFade = Math.max(300, r.width * 0.9)
        flyFade = Math.max(560, window.innerWidth * 0.6)
    }

    const tick = (now) => {
        raf = requestAnimationFrame(tick)
        if (!flying || !activeEl) {
            render()
            return
        }
        const dt = clamp(now - (lastT || now - 16.6), 8, 33)
        lastT = now
        Matter.Engine.update(engine, dt)
        const dx = body.position.x - restCenter.x
        const dy = body.position.y - restCenter.y
        const dist = Math.hypot(dx, dy)
        const op = clamp(1 - dist / flyFade, 0, 1)
        activeEl.style.transform = `translate3d(${dx}px, ${dy}px, 0) rotate(${(body.angle * 180) / Math.PI
            }deg)`
        activeEl.style.opacity = String(op)
        if (op <= 0.02) recycle()
    }

    const onDown = (e) => {
        if (!cards.length || flying) return
        if (e.pointerType === "mouse" && e.button !== 0) return
        activeEl = cards[0]
        activeEl.style.transition = ""
        pid = e.pointerId
        sx = e.clientX
        sy = e.clientY
        drag = { dx: 0, dy: 0 }
        samples = [{ x: e.clientX, y: e.clientY, t: performance.now() }]
        axis = null
        moved = false
        dragging.value = true
        window.addEventListener("pointermove", onMove, { passive: false })
        window.addEventListener("pointerup", onUp)
        window.addEventListener("pointercancel", onUp)
    }

    const onMove = (e) => {
        if (e.pointerId !== pid || !activeEl) return
        const dx = e.clientX - sx
        const dy = e.clientY - sy
        if (!axis) {
            if (Math.abs(dx) < 6 && Math.abs(dy) < 6) return
            axis = Math.abs(dx) > Math.abs(dy) ? "x" : "y"
            if (axis === "y") return cleanup()
        }
        if (axis !== "x") return
        if (e.cancelable) e.preventDefault()
        drag = { dx, dy }
        moved = moved || Math.abs(dx) > 8
        activeEl.style.transform = `translate3d(${dx}px, ${dy * 0.25}px, 0) rotate(${dx * 0.06
            }deg)`
        activeEl.style.opacity = String(clamp(1 - Math.abs(dx) / dragFade, 0, 1))
        const now = performance.now()
        samples.push({ x: e.clientX, y: e.clientY, t: now })
        if (samples.length > 6) samples.shift()
    }

    const velocity = () => {
        if (samples.length < 2) return { vx: 0, vy: 0 }
        const a = samples[0]
        const b = samples[samples.length - 1]
        const dt = b.t - a.t || 16
        return { vx: (b.x - a.x) / dt, vy: (b.y - a.y) / dt } // px per ms
    }

    const onUp = () => {
        if (!activeEl) return cleanup()
        const { vx, vy } = velocity()
        const speed = Math.hypot(vx, vy)
        if (Math.abs(drag.dx) > THRESHOLD || speed > 0.55) throwCard(vx, vy)
        else snapBack()
        cleanup()
    }

    const cleanup = () => {
        dragging.value = false
        pid = null
        axis = null
        window.removeEventListener("pointermove", onMove)
        window.removeEventListener("pointerup", onUp)
        window.removeEventListener("pointercancel", onUp)
    }

    const snapBack = () => {
        const el = activeEl
        el.style.transition = "transform 0.34s cubic-bezier(.2,.85,.3,1), opacity 0.34s"
        el.style.transform = slot(0)
        el.style.opacity = "1"
        setTimeout(() => {
            if (activeEl === el) {
                el.style.transition = ""
                activeEl = null
                render()
            }
        }, 350)
    }

    const throwCard = (vx, vy) => {
        flying = true
        lastT = 0
        // Direction and speed come from the release velocity; if the gesture was slow
        // we fall back to the drag direction so a deliberate swipe still throws.
        let speed = Math.hypot(vx, vy) * 16.666
        let ux = vx
        let uy = vy
        if (speed < 8) {
            speed = 14
            ux = drag.dx
            uy = drag.dy * 0.25
        }
        const mag = Math.hypot(ux, uy) || 1
        const s = Math.min(speed, MAX_SPEED)
        const vX = (ux / mag) * s
        const vY = (uy / mag) * s
        throwOrigin = {
            x: restCenter.x + drag.dx,
            y: restCenter.y + drag.dy * 0.25,
        }
        Matter.Body.setPosition(body, { ...throwOrigin })
        Matter.Body.setAngle(body, (drag.dx * 0.06 * Math.PI) / 180)
        Matter.Body.setVelocity(body, { x: vX, y: vY })
        Matter.Body.setAngularVelocity(body, clamp(vX * 0.0035, -MAX_SPIN, MAX_SPIN))
        activeEl.style.transition = ""
        // The next card steps up immediately, so the window follows it right away.
        if (cards[1]) showOutput(cards[1])
    }

    const recycle = () => {
        const el = activeEl
        flying = false
        activeEl = null
        if (!el) return
        el.style.transition = ""
        stage.value.appendChild(el)
        cards.splice(0, 1)
        cards.push(el)
        showOutput(cards[0])
        animateSettle()
    }

    // --- DOM / highlighting -----------------------------------------------------

    const vpCode = (html) => html.replace(/<pre class="([^"]*)"/, '<pre class="$1 vp-code"')

    // Each query is `{ "file": "...", "rust": [...], "Postgres": [...], ... }`.
    const dialects = (q) =>
        Object.keys(q).filter((k) => k !== "file" && k !== "rust")

    const cardHtml = (q, hl) => {
        const html = vpCode(
            hl.codeToHtml(code(q.rust), {
                lang: "rust",
                themes: { light: "github-light", dark: "github-dark" },
                defaultColor: false,
            }),
        )
        return `<div class="tk-card">
    <div class="tk-card__bar"><span class="tk-card__dot"></span><span class="tk-card__name">${esc(
            q.file,
        )}</span></div>
    <div class="tk-card__code">${html}</div>
  </div>`
    }

    const outputHtml = (q, hl, id) => {
        const names = dialects(q)
        const tabs = names
            .map(
                (name, i) =>
                    `<input type="radio" name="group-${id}" id="tab-${id}-${i}"${i === 0 ? " checked" : ""
                    }><label data-title="${esc(name)}" for="tab-${id}-${i}">${esc(name)}</label>`,
            )
            .join("")
        const blocks = names
            .map((name, i) => {
                const html = vpCode(
                    hl.codeToHtml(code(q[name]), {
                        lang: "sql",
                        themes: { light: "github-light", dark: "github-dark" },
                        defaultColor: false,
                    }),
                )
                return `<div class="language-sql vp-adaptive-theme${i === 0 ? " active" : ""
                    }">${html}</div>`
            })
            .join("")
        return `<div class="tk-output" data-title="Generated SQL">
    <div class="vp-code-group vp-adaptive-theme">
      <div class="tabs">${tabs}</div>
      <div class="blocks">${blocks}</div>
    </div>
  </div>`
    }

    const buildDom = (queries, hl) => {
        const stageEl = stage.value
        const outEl = outputWindow.value
        stageEl.innerHTML = ""
        outEl.innerHTML = ""
        queries.forEach((q, i) => {
            const cwrap = document.createElement("div")
            cwrap.innerHTML = cardHtml(q, hl).trim()
            const card = cwrap.firstElementChild
            stageEl.appendChild(card)

            const owrap = document.createElement("div")
            owrap.innerHTML = outputHtml(q, hl, i).trim()
            const output = owrap.firstElementChild
            const inputs = output.querySelectorAll(".tabs input")
            inputs.forEach((input, idx) => {
                input.addEventListener("change", () => {
                    output
                        .querySelectorAll(".blocks > div")
                        .forEach((b, bi) => b.classList.toggle("active", bi === idx))
                })
            })
            outEl.appendChild(output)
            cardOutputs.set(card, output)
        })
    }

    onMounted(async () => {
        if (!stage.value) return
        try {
            const base = (import.meta.env && import.meta.env.BASE_URL) || "/"
            const url = props.src || base + "queries.json"
            const [mod, data, shiki] = await Promise.all([
                import("matter-js"),
                fetch(url).then((r) => {
                    if (!r.ok) throw new Error(`Failed to load ${url}`)
                    return r.json()
                }),
                import("shiki").then(({ createHighlighter }) =>
                    createHighlighter({
                        themes: ["github-light", "github-dark"],
                        langs: ["rust", "sql"],
                    }),
                ),
            ])
            Matter = mod.default || mod
            buildDom(data.queries, shiki)

            engine = Matter.Engine.create({ gravity: { x: 0, y: 0 } })
            body = Matter.Bodies.rectangle(0, 0, 320, 520, { frictionAir: 0 })
            Matter.Composite.add(engine.world, body)

            cards = Array.from(stage.value.querySelectorAll(".tk-card"))
            measure()
            render()
            showOutput(cards[0])
            raf = requestAnimationFrame(tick)
            window.addEventListener("resize", measure)
        } catch (e) {
            console.error(e)
            failed.value = true
        } finally {
            loading.value = false
        }
    })

    onBeforeUnmount(() => {
        if (raf) cancelAnimationFrame(raf)
        cleanup()
        window.removeEventListener("resize", measure)
    })
</script>

<template>
    <div class="tk-deck">
        <div class="tk-deck__main">
            <div ref="stage" class="tk-deck__stage" :class="{ 'is-dragging': dragging }" @pointerdown="onDown"></div>

            <div class="tk-deck__arrow" aria-hidden="true">
                <span>executes</span>
                <b>&rarr;</b>
            </div>

            <div class="tk-outwin">
                <div class="tk-outwin__bar">
                    <span class="tk-outwin__title">Generated SQL</span>
                    <span class="tk-outwin__dots"><i></i><i></i><i></i></span>
                </div>
                <div class="tk-outwin__body">
                    <div ref="outputWindow" class="tk-deck__output"></div>
                </div>
            </div>
        </div>
        <p v-if="loading" class="tk-deck__hint">Loading operations…</p>
        <p v-else-if="failed" class="tk-deck__hint">Could not load the operations list.</p>
        <p v-else class="tk-deck__hint">
            Drag a card to throw it &nbsp;·&nbsp; the generated SQL follows the top card
        </p>
    </div>
</template>

<style scoped>
    .tk-deck {
        margin: 28px 0 8px;
    }

    .tk-deck__main {
        display: grid;
        grid-template-columns: 1fr;
        gap: 18px;
    }

    .tk-deck__stage {
        position: relative;
        z-index: 2;
        height: 460px;
        cursor: grab;
        touch-action: pan-y;
        user-select: none;
    }

    .tk-deck__stage.is-dragging {
        cursor: grabbing;
    }

    /* Cards are built at runtime, so they need :deep() to pick up scoped styles. */
    .tk-deck__stage :deep(.tk-card) {
        position: absolute;
        inset: 0;
        display: flex;
        flex-direction: column;
        border: 1px solid var(--vp-c-divider);
        border-radius: 16px;
        overflow: hidden;
        background: var(--vp-code-block-bg, var(--vp-c-bg-soft));
        box-shadow: 0 26px 60px -30px rgba(0, 0, 0, 0.7);
        transform-origin: 50% 135%;
        will-change: transform, opacity;
    }

    .tk-deck__stage :deep(.tk-card__bar) {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 10px 14px;
        border-bottom: 1px solid var(--vp-c-divider);
        background: var(--vp-c-bg-alt);
    }

    .tk-deck__stage :deep(.tk-card__dot) {
        width: 8px;
        height: 8px;
        border-radius: 2px;
        background: var(--vp-c-brand-1);
        opacity: 0.85;
    }

    .tk-deck__stage :deep(.tk-card__name) {
        font-family: ui-monospace, "Courier New", monospace;
        font-size: 12px;
        letter-spacing: 0.03em;
        color: var(--vp-c-text-2);
    }

.tk-deck__stage :deep(.tk-card__code) {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  overflow-x: hidden;
  scrollbar-width: thin;
}

.tk-deck__stage :deep(.tk-card__code)::-webkit-scrollbar {
  width: 6px;
}

.tk-deck__stage :deep(.tk-card__code)::-webkit-scrollbar-thumb {
  border-radius: 3px;
  background: var(--vp-c-divider);
}

    .tk-deck__stage :deep(.tk-card__code div[class*="language-"]) {
        margin: 0;
        border-radius: 0;
        background: transparent;
    }

.tk-deck__stage :deep(.tk-card__code pre) {
  margin: 0;
  background: transparent;
  overflow: visible;
}

    .tk-deck__stage :deep(.tk-card__code .lang) {
        display: none;
    }

.tk-deck__stage :deep(.tk-card__code pre),
.tk-deck__stage :deep(.tk-card__code code) {
  font-size: 11.5px;
  line-height: 1.5;
}

    @media (min-width: 960px) {
        .tk-deck__main {
            grid-template-columns: 1.05fr 88px 1fr;
            gap: 0;
            align-items: center;
        }

  .tk-deck__stage {
    height: 660px;
    overflow: visible;
  }
    }

    .tk-deck__arrow {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        color: var(--vp-c-brand-1);
        font-size: 12px;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        font-weight: 700;
    }

    .tk-deck__arrow b {
        font-size: 22px;
        transform: rotate(90deg);
        line-height: 1;
    }

    @media (min-width: 960px) {
        .tk-deck__arrow {
            flex-direction: column;
            gap: 10px;
        }

        .tk-deck__arrow span {
            writing-mode: vertical-rl;
            transform: rotate(180deg);
        }

        .tk-deck__arrow b {
            transform: none;
        }
    }

    .tk-outwin {
        display: flex;
        flex-direction: column;
        height: 300px;
        border: 1px solid var(--vp-c-divider);
        border-radius: 14px;
        overflow: hidden;
        background: var(--vp-code-block-bg, var(--vp-c-bg-soft));
        box-shadow: 0 24px 50px -32px rgba(0, 0, 0, 0.65);
    }

@media (min-width: 960px) {
  .tk-outwin {
    height: 660px;
  }
}

    .tk-outwin__bar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding: 11px 14px;
        background: var(--vp-c-bg-alt);
        border-bottom: 1px solid var(--vp-c-divider);
    }

    .tk-outwin__title {
        font-family: ui-monospace, "Courier New", monospace;
        font-size: 12px;
        letter-spacing: 0.04em;
        color: var(--vp-c-text-2);
    }

    .tk-outwin__dots {
        display: inline-flex;
        gap: 6px;
    }

    .tk-outwin__dots i {
        display: block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background: var(--vp-c-divider);
    }

    .tk-outwin__body {
        flex: 1;
        min-height: 0;
    }

    .tk-deck__output {
        height: 100%;
    }

    .tk-deck__output :deep(.tk-output) {
        display: none;
        height: 100%;
    }

    .tk-deck__output :deep(.tk-output.is-active) {
        display: block;
    }

    .tk-deck__output :deep(.vp-code-group) {
        display: flex;
        flex-direction: column;
        height: 100%;
        margin: 0;
        border-radius: 0;
        box-shadow: none;
    }

    .tk-deck__output :deep(.vp-code-group .tabs) {
        background: var(--vp-c-bg-alt);
        border-bottom: 1px solid var(--vp-c-divider);
        box-shadow: none;
        flex-shrink: 0;
    }

    .tk-deck__output :deep(.vp-code-group .blocks) {
        flex: 1;
        min-height: 0;
        border-radius: 0;
    }

    .tk-deck__output :deep(.vp-code-group .blocks > div[class*="language-"]) {
        height: 100%;
        margin: 0;
        border-radius: 0;
        background: var(--vp-code-block-bg, var(--vp-c-bg-soft));
    }

    .tk-deck__output :deep(pre) {
        height: 100%;
        max-height: none;
        margin: 0;
        overflow: auto;
        background: transparent;
    }

    .tk-deck__output :deep(.lang) {
        display: none;
    }

    .tk-deck__hint {
        margin: 16px 0 0;
        text-align: center;
        font-size: 12px;
        letter-spacing: 0.06em;
        color: var(--vp-c-text-3);
    }
</style>
