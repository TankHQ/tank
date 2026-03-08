<script setup>
import { ref, onMounted, onUnmounted } from 'vue'

const jokes = ref([])
const currentJoke = ref(null)
let intervalId = null

const fetchJokes = async () => {
    try {
        const response = await fetch('/tank/jokes.json')
        const data = await response.json()
        jokes.value = data.jokes
        setRandomJoke()
    } catch (error) {
        console.error('Failed to load:', error)
    }
}

const setRandomJoke = () => {
    if (jokes.value.length === 0) return
    const randomIndex = Math.floor(Math.random() * jokes.value.length)
    currentJoke.value = jokes.value[randomIndex]
    resetTimer()
}

const resetTimer = () => {
    if (intervalId) clearInterval(intervalId)
    intervalId = setInterval(setRandomJoke, 30000)
}

onMounted(() => {
    fetchJokes()
})

onUnmounted(() => {
    if (intervalId) clearInterval(intervalId)
})
</script>

<template>
    <div class="tank-joke">
        <div v-if="currentJoke">
            <p class="title">{{ currentJoke.title }}</p>
            <p class="body">{{ currentJoke.body }}</p>
            <button @click="setRandomJoke" class="new-joke">Get a joke</button>
        </div>
        <div v-else>
            <p>Loading...</p>
        </div>
    </div>
</template>

<style scoped>
.tank-joke {}

.title {
    font-size: 1.25rem;
    font-style: italic;
    color: var(--vp-c-text-1);
}

.body {
    font-size: 1.1rem;
    color: var(--vp-c-text-2);
}

.new-joke {}

.new-joke:hover {}
</style>