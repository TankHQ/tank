<script setup>
    import { onBeforeUnmount, onMounted, ref } from "vue"
    import { useDrag } from "./useDrag.js"

    const drivers = [
        "Postgres",
        "SQLite",
        "MySQL",
        "MariaDB",
        "DuckDB",
        "ClickHouse",
        "chDB",
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

    const measure = () => {
        if (!track.value) return
        width = track.value.scrollWidth / 2
    }
    const loop = () => {
        if (!paused.value) offset -= 0.6
        if (offset <= -width) offset += width
        if (offset > 0) offset -= width
        if (track.value) track.value.style.transform = `translateX(${offset}px)`
        raf = requestAnimationFrame(loop)
    }
    const start = () => {
        measure()
        if (!raf) raf = requestAnimationFrame(loop)
    }
    const stop = () => {
        if (raf) cancelAnimationFrame(raf)
        raf = null
    }

    const drag = useDrag({
        onStart: () => {
            paused.value = true
            startOffset = offset
            if (track.value) track.value.classList.add("is-grabbed")
        },
        onMove: ({ dx, moved }) => {
            if (moved) offset = startOffset + dx
        },
        onEnd: () => {
            paused.value = false
            if (track.value) track.value.classList.remove("is-grabbed")
        },
    })

    onMounted(() => {
        measure()
        raf = requestAnimationFrame(loop)
    })

    onBeforeUnmount(stop)
</script>

<template>
    <div class="tk-marquee">
        <span class="tk-marquee__label">Supports</span>
        <div class="tk-marquee__viewport" @pointerdown="drag.down" @pointerenter="paused = true"
            @pointerleave="paused = false" @click.capture="drag.suppressClick">
            <div ref="track" class="tk-marquee__track">
                <span v-for="(d, i) in [...drivers, ...drivers]" :key="i" class="tk-marquee__item">
                    {{ d }}
                </span>
            </div>
        </div>
    </div>
</template>

<style scoped>
    .tk-marquee {
        display: flex;
        flex-direction: column;
        gap: 12px;
        margin: 28px 0 8px;
    }

    @media (min-width: 720px) {
        .tk-marquee {
            flex-direction: row;
            align-items: center;
            gap: 20px;
        }
    }

    .tk-marquee__label {
        flex-shrink: 0;
        font-size: 12px;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        font-weight: 700;
        color: var(--vp-c-text-3);
    }

    .tk-marquee__viewport {
        position: relative;
        overflow: hidden;
        flex: 1;
        cursor: grab;
        user-select: none;
        touch-action: pan-y;
        mask-image: linear-gradient(90deg, transparent, #000 8%, #000 92%, transparent);
        -webkit-mask-image: linear-gradient(90deg, transparent, #000 8%, #000 92%, transparent);
    }

    .tk-marquee__viewport:active {
        cursor: grabbing;
    }

    .tk-marquee__track {
        display: flex;
        width: max-content;
        gap: 10px;
        will-change: transform;
    }

    .tk-marquee__item {
        flex-shrink: 0;
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
