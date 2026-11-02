<script setup>
import { onMounted, onUnmounted, ref } from "vue"

const jokes = ref([])
const currentJoke = ref(null)
let timer = null

const nextJoke = () => {
    if (!jokes.value.length) return
    currentJoke.value = jokes.value[Math.floor(Math.random() * jokes.value.length)]
    clearInterval(timer)
    timer = setInterval(nextJoke, 30000)
}

onMounted(async () => {
    const res = await fetch("/tank/jokes.json")
    jokes.value = (await res.json()).jokes
    nextJoke()
})

onUnmounted(() => clearInterval(timer))
</script>

<template>
    <div v-if="currentJoke" class="tank-joke">
        <p class="title">{{ currentJoke.title }}</p>
        <p class="body">{{ currentJoke.body }}</p>
        <button class="VPButton medium alt" @click="nextJoke">Get a joke</button>
    </div>
</template>

<style scoped>
.title {
    font-size: 1.25rem;
    font-style: italic;
    color: var(--vp-c-text-1);
}

.body {
    font-size: 1.1rem;
    color: var(--vp-c-text-2);
}
</style>
