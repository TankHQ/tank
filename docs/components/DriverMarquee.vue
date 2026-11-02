<script setup>
import { onBeforeUnmount, onMounted, ref } from "vue"
import { useDrag } from "./useDrag.js"

const drivers = [
    "Postgres",
    "SQLite",
    "MySQL",
    "MariaDB",
    "DuckDB",
    "MongoDB",
    "ScyllaDB",
    "Cassandra",
    "Valkey",
    "Redis",
]

const track = ref(null)
const paused = ref(false)
let offset = 0
let startOffset = 0
let width = 0
let raf = null

const loop = () => {
    if (!paused.value) offset -= 0.6
    if (width) offset = ((offset % width) - width) % width
    if (track.value) track.value.style.transform = `translateX(${offset}px)`
    raf = requestAnimationFrame(loop)
}

const { down } = useDrag({
    onStart: () => {
        paused.value = true
        startOffset = offset
    },
    onMove: ({ dx }) => {
        offset = startOffset + dx
    },
    onEnd: () => {
        paused.value = false
    },
})

onMounted(() => {
    width = (track.value?.scrollWidth ?? 0) / 2
    raf = requestAnimationFrame(loop)
})

onBeforeUnmount(() => cancelAnimationFrame(raf))
</script>

<template>
    <div class="driver-marquee">
        <strong>Supports</strong>
        <div @pointerdown="down" @pointerenter="paused = true" @pointerleave="paused = false">
            <ul ref="track">
                <li v-for="(d, i) in [...drivers, ...drivers]" :key="i">{{ d }}</li>
            </ul>
        </div>
    </div>
</template>

<style scoped>
.driver-marquee {
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin: 28px 0 8px;
}

@media (min-width: 720px) {
    .driver-marquee {
        flex-direction: row;
        align-items: center;
        gap: 20px;
    }
}

strong {
    font-size: 12px;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--vp-c-text-3);
}

.driver-marquee > div {
    overflow: hidden;
    flex: 1;
    cursor: grab;
    user-select: none;
    touch-action: pan-y;
    mask-image: linear-gradient(90deg, transparent, #000 8%, #000 92%, transparent);
}

.driver-marquee > div:active {
    cursor: grabbing;
}

ul {
    display: flex;
    width: max-content;
    gap: 10px;
    margin: 0;
    padding: 0;
    list-style: none;
    will-change: transform;
}

li {
    margin: 0;
    padding: 5px 14px;
    border: 1px solid var(--vp-c-divider);
    border-radius: 999px;
    background: var(--vp-c-bg-soft);
    color: var(--vp-c-text-2);
    font-size: 13px;
    font-weight: 600;
    white-space: nowrap;
}
</style>
