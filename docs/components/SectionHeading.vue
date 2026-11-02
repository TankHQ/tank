<script setup>
defineProps({ eyebrow: String, title: String })
</script>

<template>
    <header class="section-heading">
        <small v-if="eyebrow">{{ eyebrow }}</small>
        <h2>{{ title }}</h2>
        <p v-if="$slots.default"><slot /></p>
    </header>
</template>

<style scoped>
.section-heading {
    margin: 80px 0 24px;
}

small {
    display: inline-flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
    font: 700 12px var(--vp-font-family-mono);
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--vp-c-brand-1);
}

small::before {
    content: "";
    width: 28px;
    height: 2px;
    background: var(--vp-c-brand-1);
}

h2 {
    margin: 0;
    padding: 0;
    border: none;
    font-family: RobotoSlab-Bold, var(--vp-font-family-base);
    font-size: clamp(28px, 4vw, 40px);
    line-height: 1.15;
    letter-spacing: -0.01em;
    color: var(--vp-c-text-1);
}

p {
    max-width: 720px;
    margin: 14px 0 0;
    font-size: 17px;
    line-height: 1.7;
    color: var(--vp-c-text-2);
    text-align: left;
}
</style>
