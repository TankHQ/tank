// https://vitepress.dev/guide/custom-theme
import DefaultTheme from "vitepress/theme-without-fonts"
import "./style.css"

/** @type {import("vitepress").Theme} */
export default {
    extends: DefaultTheme,
    enhanceApp() {
        if (typeof window === "undefined") return
        const setup = () =>
            document.querySelectorAll(".sticky-table:not([data-sticky-setup])").forEach((el) => {
                el.dataset.stickySetup = "true"
                el.addEventListener("scroll", () => el.classList.toggle("is-scrolled", el.scrollLeft > 0))
            })
        if (document.readyState === "loading") {
            document.addEventListener("DOMContentLoaded", setup)
        } else {
            setup()
        }
        new MutationObserver(setup).observe(document.body, { childList: true, subtree: true })
    },
}
