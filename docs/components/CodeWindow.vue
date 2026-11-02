<script setup>
    defineProps({
        title: String,
    })
</script>

<template>
    <figure class="tk-window">
        <figcaption class="tk-window__bar">
            <span class="tk-window__title">{{ title }}</span>
            <span class="tk-window__dots"><i></i><i></i><i></i></span>
        </figcaption>
        <div class="tk-window__body">
            <slot />
        </div>
    </figure>
</template>

<style scoped>
    .tk-window {
        margin: 0;
        height: 100%;
        border: 1px solid var(--vp-c-divider);
        border-radius: 14px;
        overflow: hidden;
        background: var(--vp-c-bg-soft);
        box-shadow: 0 24px 50px -32px rgba(0, 0, 0, 0.65);
    }

    .tk-window__bar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding: 11px 14px;
        background: var(--vp-c-bg-alt);
        border-bottom: 1px solid var(--vp-c-divider);
    }

    .tk-window__dots {
        display: inline-flex;
        gap: 6px;
    }

    .tk-window__dots i {
        display: block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background: var(--vp-c-divider);
    }

    .tk-window__title {
        font-family: ui-monospace, "Courier New", monospace;
        font-size: 12px;
        letter-spacing: 0.04em;
        color: var(--vp-c-text-2);
    }

    .tk-window__body {
        min-width: 0;
    }

    .tk-window__body :deep(div[class*="language-"]) {
        margin: 0;
        border-radius: 0;
        background: transparent;
    }

    .tk-window__body :deep(div[class*="language-"] pre) {
        background: transparent;
    }

    .tk-window__body :deep(.lang) {
        display: none;
    }

    .tk-window__body :deep(.vp-code-group) {
        margin: 0;
        border-radius: 0;
        box-shadow: none;
    }

    .tk-window__body :deep(.vp-code-group .tabs) {
        background: var(--vp-c-bg-alt);
        border-bottom: 1px solid var(--vp-c-divider);
    }

    .tk-window__body :deep(.vp-code-group .blocks) {
        border-radius: 0;
    }
</style>
