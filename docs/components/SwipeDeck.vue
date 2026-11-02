<script setup>
import { Comment, cloneVNode, ref, useSlots } from "vue"
import CodeWindow from "./CodeWindow.vue"
import { useDrag } from "./useDrag.js"

const VISIBLE = 5
const slots = useSlots()
const top = ref(0)
const drag = ref({ x: 0, y: 0, active: false })
const thrown = ref(null)

const nodes = () => (slots.default?.() ?? []).filter((v) => v.type !== Comment)
const cardNodes = () => nodes().filter((v) => v.type === CodeWindow)
const sqlNodes = () => nodes().filter((v) => v.type !== CodeWindow)

const cardStyle = (i, total) => {
    if (thrown.value?.index === i) {
        const { x, y, a } = thrown.value
        return {
            zIndex: total + 1,
            opacity: 0,
            transform: `translate3d(${x}px, ${y}px, 0) rotate(${a}deg)`,
            transition: "transform 0.42s ease-out, opacity 0.42s ease-out",
        }
    }
    const d = (i - top.value + total) % total
    if (d === 0 && drag.value.active) {
        const { x, y } = drag.value
        return {
            zIndex: total,
            opacity: Math.max(0, 1 - Math.abs(x) / 420),
            transform: `translate3d(${x}px, ${y * 0.25}px, 0) rotate(${x * 0.06}deg)`,
            transition: "none",
        }
    }
    const s = Math.min(d, VISIBLE)
    return {
        zIndex: total - d,
        opacity: d < VISIBLE ? 1 : 0,
        pointerEvents: d === 0 ? "auto" : "none",
        transform: `translate3d(${-s * 7}px, ${-s * 13}px, 0) rotate(${-s * 1.6}deg) scale(${1 - s * 0.035})`,
        transition: d < VISIBLE ? undefined : "none",
    }
}

const Cards = () => cardNodes().map((v, i, a) => cloneVNode(v, { style: cardStyle(i, a.length) }))
const Groups = () => sqlNodes().map((v, i) => cloneVNode(v, { class: { active: i === top.value } }))

const { dragging, down } = useDrag({
    onMove: ({ dx, dy }) => {
        drag.value = { x: dx, y: dy, active: true }
    },
    onEnd: ({ dx, dy, vx, vy, moved }) => {
        const count = cardNodes().length
        const speed = Math.hypot(vx, vy)
        if (moved && count && (Math.abs(dx) > 120 || speed > 0.55)) {
            const ux = speed > 0.5 ? vx : Math.sign(dx || 1) * 0.9
            const uy = speed > 0.5 ? vy : (dy / (Math.abs(dx) || 1)) * 0.2
            const index = top.value
            thrown.value = { index, x: dx + ux * 480, y: dy * 0.25 + uy * 480, a: dx * 0.06 + ux * 28 }
            top.value = (top.value + 1) % count
            setTimeout(() => {
                if (thrown.value?.index === index) thrown.value = null
            }, 420)
        }
        drag.value = { x: 0, y: 0, active: false }
    },
})
</script>

<template>
    <section class="swipe-deck">
        <div class="stage" :class="{ dragging }" @pointerdown="down">
            <Cards />
        </div>
        <div class="arrow" aria-hidden="true">
            <span>executes</span>
            <b>&rarr;</b>
        </div>
        <CodeWindow title="Generated SQL">
            <Groups />
        </CodeWindow>
        <p class="hint">Drag a card to throw it &nbsp;·&nbsp; the generated SQL follows the top card</p>
    </section>
</template>

<style scoped>
.swipe-deck {
    display: grid;
    grid-template-columns: 1fr;
    gap: 18px;
    margin: 28px 0 8px;
}

.stage {
    position: relative;
    z-index: 2;
    height: 460px;
    cursor: grab;
    touch-action: pan-y;
    user-select: none;
}

.stage.dragging {
    cursor: grabbing;
}

.stage :deep(.code-window) {
    position: absolute;
    inset: 0;
    transform-origin: 50% 135%;
    transition: transform 0.38s cubic-bezier(0.2, 0.8, 0.25, 1), opacity 0.38s;
    will-change: transform, opacity;
}

.swipe-deck > .code-window {
    height: 300px;
}

.swipe-deck > .code-window :deep(.vp-code-group:not(.active)) {
    display: none;
}

.arrow {
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

.arrow b {
    font-size: 22px;
    transform: rotate(90deg);
    line-height: 1;
}

.hint {
    grid-column: 1 / -1;
    margin: 0;
    text-align: center;
    font-size: 12px;
    letter-spacing: 0.06em;
    color: var(--vp-c-text-3);
}

@media (min-width: 960px) {
    .swipe-deck {
        grid-template-columns: 1.05fr 88px 1fr;
        row-gap: 16px;
        align-items: center;
    }

    .stage,
    .swipe-deck > .code-window {
        height: 660px;
    }

    .arrow {
        flex-direction: column;
        gap: 10px;
    }

    .arrow span {
        writing-mode: vertical-rl;
        transform: rotate(180deg);
    }

    .arrow b {
        transform: none;
    }
}
</style>
