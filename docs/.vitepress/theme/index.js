// https://vitepress.dev/guide/custom-theme
import DefaultTheme from "vitepress/theme-without-fonts"
import HomeComponents from "../../components/GlobalComponents.js"
import "./style.css"

/** @type {import("vitepress").Theme} */
export default {
    extends: DefaultTheme,
    enhanceApp({ app, router, siteData }) {
        app.use(HomeComponents)
        if (typeof window !== 'undefined') {
            const setupStickyTables = () => {
                const stickyTables = document.querySelectorAll('.sticky-table');
                stickyTables.forEach(table => {
                    if (table.dataset.stickySetup) return;
                    table.dataset.stickySetup = 'true';
                    const handleScroll = () => {
                        if (table.scrollLeft > 0) {
                            table.classList.add('is-scrolled');
                        } else {
                            table.classList.remove('is-scrolled');
                        }
                    };
                    table.addEventListener('scroll', handleScroll);
                });
            };
            if (document.readyState === 'loading') {
                document.addEventListener('DOMContentLoaded', setupStickyTables);
            } else {
                setupStickyTables();
            }
            const observer = new MutationObserver(() => {
                setupStickyTables();
            });
            observer.observe(document.body, {
                childList: true,
                subtree: true
            });
        }
    }
}
