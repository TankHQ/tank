<script setup>
defineProps({ title: String })
</script>

<template>
    <figure class="code-window">
        <figcaption v-if="title">{{ title }}</figcaption>
        <slot />
    </figure>
</template>

<style scoped>
.code-window {
    display: flex;
    flex-direction: column;
    margin: 0;
    border: 1px solid var(--vp-c-divider);
    border-radius: 14px;
    overflow: hidden;
    background: var(--vp-code-block-bg);
    box-shadow: 0 24px 50px -32px rgba(0, 0, 0, 0.65);
}

figcaption {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 14px;
    background: var(--vp-c-bg-alt);
    border-bottom: 1px solid var(--vp-c-divider);
    font: 12px var(--vp-font-family-mono);
    color: var(--vp-c-text-2);
}

figcaption::after {
    content: "";
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: var(--vp-c-divider);
    box-shadow: -16px 0 var(--vp-c-divider), -32px 0 var(--vp-c-divider);
}

.code-window :deep(.vp-code-group),
.code-window :deep(.blocks) {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
    margin: 0;
}

.code-window :deep(.tabs) {
    margin: 0;
    border-radius: 0;
    background: var(--vp-c-bg-alt);
}

.code-window :deep(div[class*="language-"]) {
    flex: 1;
    min-height: 0;
    margin: 0;
    border-radius: 0;
    overflow: auto;
}

.code-window :deep(pre) {
    overflow: visible;
}

.code-window :deep(.lang) {
    display: none;
}
</style>
