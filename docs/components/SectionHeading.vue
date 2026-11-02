<script setup>
    defineProps({
        eyebrow: String,
        title: String,
    })
</script>

<template>
    <header class="tk-head">
        <p v-if="eyebrow" class="tk-head__eyebrow">{{ eyebrow }}</p>
        <h2 class="tk-head__title">{{ title }}</h2>
        <p v-if="$slots.default" class="tk-head__lead">
            <slot />
        </p>
    </header>
</template>

<style scoped>
    .tk-head {
        margin: 80px 0 24px;
    }

    .tk-head__eyebrow {
        display: inline-flex;
        align-items: center;
        gap: 10px;
        margin: 0 0 12px;
        font-family: ui-monospace, "Courier New", monospace;
        font-size: 12px;
        letter-spacing: 0.2em;
        text-transform: uppercase;
        font-weight: 700;
        color: var(--vp-c-brand-1);
    }

    .tk-head__eyebrow::before {
        content: "";
        width: 28px;
        height: 2px;
        background: var(--vp-c-brand-1);
    }

    .tk-head__title {
        margin: 0;
        padding: 0;
        border: none;
        font-family: RobotoSlab-Bold, var(--vp-font-family-base);
        font-size: clamp(28px, 4vw, 40px);
        line-height: 1.15;
        letter-spacing: -0.01em;
        color: var(--vp-c-text-1);
    }

    .tk-head__lead {
        max-width: 720px;
        margin: 14px 0 0;
        font-size: 17px;
        line-height: 1.7;
        color: var(--vp-c-text-2);
        text-align: left;
    }
</style>
