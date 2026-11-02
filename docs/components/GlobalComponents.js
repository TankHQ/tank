import CodeWindow from "./CodeWindow.vue"
import DriverMarquee from "./DriverMarquee.vue"
import HomeStats from "./HomeStats.vue"
import SectionHeading from "./SectionHeading.vue"
import SwipeDeck from "./SwipeDeck.vue"

const components = {
    CodeWindow,
    DriverMarquee,
    HomeStats,
    SectionHeading,
    SwipeDeck,
}

export default {
    install(app) {
        for (const [name, component] of Object.entries(components)) {
            app.component(name, component)
        }
    },
}
