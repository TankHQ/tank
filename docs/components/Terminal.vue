<script setup>
    import { onBeforeUnmount, ref } from "vue"

    const root = ref(null)
    let groups = []
    let entries = []
    let g = 0
    let b = 0
    let k = 0
    let raf = 0
    let lastT = 0
    let cursor = null

    const render = (entry, count, typing) => {
        if (!entry) return
        let rem = count
        let last = entry.code
        for (const { node, text } of entry.nodes) {
            const take = Math.min(rem, text.length)
            node.nodeValue = text.slice(0, take)
            if (take > 0) last = node
            rem -= take
        }
        cursor.classList.toggle("typing", typing)
        if (last === entry.code) entry.code.prepend(cursor)
        else last.after(cursor)
    }

    const tick = (now) => {
        const entry = entries[g]?.[b]
        if (!entry) return (raf = 0)
        const dt = Math.min(50, Math.max(8, now - (lastT || now - 16)))
        lastT = now
        k = Math.min(entry.total, k + dt * Math.max(0.1, entry.total / 1250))
        const more = k < entry.total
        render(entry, Math.floor(k), more)
        raf = more ? requestAnimationFrame(tick) : 0
    }

    const run = (gIdx, bIdx) => {
        cancelAnimationFrame(raf)
        g = gIdx
        b = bIdx
        k = 0
        lastT = 0
        groups.forEach((el, i) => el.classList.toggle("active", i === g))
        const inputs = [...(groups[g]?.querySelectorAll(".tabs input") ?? [])]
        if (inputs[b]) inputs[b].checked = true
        entries[g]?.forEach((e, i) => e.block.classList.toggle("active", i === b))
        render(entries[g]?.[b], 0, true)
        raf = requestAnimationFrame(tick)
    }

    const init = (els) => {
        cursor = document.createElement("span")
        cursor.className = "cursor"
        groups = els
        groups.forEach((el) => root.value.appendChild(el))
        entries = groups.map((el) =>
            [...el.querySelectorAll(".blocks > div[class*='language-']")].map((block) => {
                const code = block.querySelector("code")
                const nodes = []
                const walker = document.createTreeWalker(code, NodeFilter.SHOW_TEXT)
                let n
                while ((n = walker.nextNode())) nodes.push({ node: n, text: n.nodeValue })
                return { block, code, nodes, total: nodes.reduce((s, x) => s + x.text.length, 0) }
            }),
        )
        run(0, 0)
    }

    const show = (gIdx) => {
        const db = groups[g]?.querySelectorAll(".tabs label")?.[b]?.dataset.title
        const labels = [...(groups[gIdx]?.querySelectorAll(".tabs label") ?? [])]
        run(gIdx, Math.max(0, labels.findIndex((l) => l.dataset.title === db)))
    }

    const click = (e) => {
        const input = e.target.closest(".tabs input")
        if (!input) return
        const bIdx = [...(groups[g]?.querySelectorAll(".tabs input") ?? [])].indexOf(input)
        if (bIdx >= 0) run(g, bIdx)
    }

    onBeforeUnmount(() => cancelAnimationFrame(raf))

    defineExpose({ init, show })
</script>

<template>
    <div ref="root" class="terminal" @click="click" />
</template>
