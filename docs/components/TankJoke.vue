<script setup>
    import { onMounted, onUnmounted, ref } from "vue"

    const joke = ref(null)
    let jokes = []
    let timer = null

    const next = () => {
        if (!jokes.length) return
        joke.value = jokes[Math.floor(Math.random() * jokes.length)]
        clearInterval(timer)
        timer = setInterval(next, 30000)
    }

    onMounted(async () => {
        jokes = (await (await fetch("/tank/jokes.json")).json()).jokes
        next()
    })

    onUnmounted(() => clearInterval(timer))
</script>

<template>
    <div v-if="joke" class="tank-joke">
        <p><em>{{ joke.title }}</em></p>
        <p>{{ joke.body }}</p>
        <button class="VPButton medium alt" @click="next">Get a joke</button>
    </div>
</template>
