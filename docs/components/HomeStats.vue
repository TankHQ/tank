<script setup>
    const stats = [
        { value: "9", label: "Database backends" },
        { value: "1", label: "Entity model" },
        { value: "0", label: "Boilerplate" },
        { value: "100%", label: "Async" },
    ]
</script>

<template>
    <div class="tk-stats">
        <div v-for="s in stats" :key="s.label" class="tk-stat">
            <span class="tk-stat__value">{{ s.value }}</span>
            <span class="tk-stat__label">{{ s.label }}</span>
        </div>
    </div>
</template>

<style scoped>
    .tk-stats {
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
        .tk-stats {
            grid-template-columns: repeat(4, 1fr);
        }
    }

    .tk-stat {
        display: flex;
        flex-direction: column;
        gap: 6px;
        padding: 22px 20px;
        background: var(--vp-c-bg-soft);
        text-align: center;
    }

    .tk-stat__value {
        font-family: RobotoSlab-Bold, var(--vp-font-family-base);
        font-size: 32px;
        line-height: 1;
        background: linear-gradient(120deg, var(--vp-c-brand-2), var(--vp-c-brand-1));
        -webkit-background-clip: text;
        background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    .tk-stat__label {
        font-size: 12px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: var(--vp-c-text-2);
    }
</style>
