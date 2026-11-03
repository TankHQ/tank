<script setup>
    import { onBeforeUnmount, onMounted, ref } from "vue"

    const root = ref(null)
    let cards = []
    let groups = []
    let top = 0
    let sx = 0
    let sy = 0
    let t0 = 0
    let axis = null
    let timer = null
    const CARDS = 6

    const sync = (skip) => {
        const n = cards.length
        cards.forEach((el, i) => {
            if (el === skip) return
            const d = (i - top + n) % n
            const s = Math.min(d, CARDS)
            Object.assign(el.style, {
                zIndex: n - d,
                opacity: d < CARDS ? 1 : 0,
                pointerEvents: d === 0 ? "auto" : "none",
                transform: `translate3d(${-s * 7}px, ${-s * 13}px, 0) rotate(${-s * 1}deg) scale(${1 - s * 0.035})`,
                transition: d < CARDS ? "" : "none",
            })
        })
        groups.forEach((el, i) => el.classList.toggle("active", i === top))
    }

    const stop = () => {
        root.value?.classList.remove("dragging")
        window.removeEventListener("pointermove", move)
        window.removeEventListener("pointerup", up)
        window.removeEventListener("pointercancel", up)
    }

    const move = (e) => {
        const dx = e.clientX - sx
        const dy = e.clientY - sy
        if (!axis) {
            if (Math.hypot(dx, dy) < 6) return
            if (Math.abs(dy) > Math.abs(dx)) return stop()
            axis = "x"
            root.value?.classList.add("dragging")
        }
        if (e.cancelable) e.preventDefault()
        Object.assign(cards[top].style, {
            opacity: Math.max(0, 1 - Math.abs(dx) / 420),
            transform: `translate3d(${dx}px, ${dy * 0.25}px, 0) rotate(${dx * 0.06}deg)`,
            transition: "none",
        })
    }

    const up = (e) => {
        const dx = e.clientX - sx
        const dy = e.clientY - sy
        const dt = Math.max(16, performance.now() - t0)
        const vx = dx / dt
        const vy = dy / dt
        const swiped = axis === "x" && (Math.abs(dx) > 120 || Math.hypot(vx, vy) > 0.55)
        stop()
        if (!swiped || !cards.length) return sync()
        const fast = Math.hypot(vx, vy) > 0.5
        const ux = fast ? vx : Math.sign(dx || 1) * 0.9
        const uy = fast ? vy : (dy / (Math.abs(dx) || 1)) * 0.2
        const card = cards[top]
        const db = groups[top]?.querySelector("input:checked + label")?.dataset.title
        Object.assign(card.style, {
            zIndex: cards.length + 1,
            opacity: 0,
            pointerEvents: "none",
            transform: `translate3d(${dx + ux * 480}px, ${dy * 0.25 + uy * 480}px, 0) rotate(${dx * 0.06 + ux * 28}deg)`,
            transition: "transform 0.42s ease-out, opacity 0.42s ease-out",
        })
        top = (top + 1) % cards.length
        const group = groups[top]
        const labels = [...(group?.querySelectorAll(".tabs label") ?? [])]
        const idx = Math.max(0, labels.findIndex((l) => l.dataset.title === db))
        if (labels[idx]?.control) labels[idx].control.checked = true
        ;[...(group?.querySelector(".blocks")?.children ?? [])].forEach((b, i) => b.classList.toggle("active", i === idx))
        sync(card)
        clearTimeout(timer)
        timer = setTimeout(sync, 420)
    }

    const down = (e) => {
        if ((e.pointerType === "mouse" && e.button !== 0) || !cards[top]?.contains(e.target) || e.target.closest("button")) return
        sx = e.clientX
        sy = e.clientY
        t0 = performance.now()
        axis = null
        window.addEventListener("pointermove", move, { passive: false })
        window.addEventListener("pointerup", up)
        window.addEventListener("pointercancel", up)
    }

    onMounted(() => {
        cards = [...root.value.querySelectorAll(":scope > div[class*='language-']")]
        groups = [...root.value.querySelectorAll(":scope > .vp-code-group")]
        for (let i = cards.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1))
            ;[cards[i], cards[j]] = [cards[j], cards[i]]
            ;[groups[i], groups[j]] = [groups[j], groups[i]]
        }
        sync()
    })

    onBeforeUnmount(() => {
        clearTimeout(timer)
        stop()
    })
</script>

<template>
    <section ref="root" class="swipe-deck" @pointerdown="down">
        <slot />
        <div class="arrow" aria-hidden="true">
            <span>executes</span>
            <b>&darr;</b>
        </div>
        <p class="hint">Drag a card to throw it &nbsp;·&nbsp; the generated SQL follows the top card</p>
    </section>
</template>
