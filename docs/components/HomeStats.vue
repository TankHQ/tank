<script setup>
const stats = [
    ["9", "Database backends"],
    ["1", "Entity model"],
    ["0", "Boilerplate"],
    ["100%", "Async"],
]
</script>

<template>
    <dl class="home-stats">
        <div v-for="[value, label] in stats" :key="label">
            <dt>{{ value }}</dt>
            <dd>{{ label }}</dd>
        </div>
    </dl>
</template>

<style scoped>
.home-stats {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 1px;
    margin: 28px 0 8px;
    border: 1px solid var(--vp-c-divider);
    border-radius: 16px;
    overflow: hidden;
    background: var(--vp-c-divider);
    box-shadow: 0 18px 40px -32px rgba(0, 0, 0, 0.6);
}

@media (min-width: 640px) {
    .home-stats {
        grid-template-columns: repeat(4, 1fr);
    }
}

div {
    padding: 22px 20px;
    background: var(--vp-c-bg-soft);
    text-align: center;
}

dt {
    font-family: RobotoSlab-Bold, var(--vp-font-family-base);
    font-size: 32px;
    line-height: 1;
    background: linear-gradient(120deg, var(--vp-c-brand-2), var(--vp-c-brand-1));
    background-clip: text;
    -webkit-text-fill-color: transparent;
}

dd {
    margin: 6px 0 0;
    font-size: 12px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--vp-c-text-2);
}
</style>
