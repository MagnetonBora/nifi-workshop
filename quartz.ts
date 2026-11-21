import { loadQuartzConfig, loadQuartzLayout } from "./quartz/plugins/loader/config-loader"
import { componentRegistry } from "./quartz/components/registry"

componentRegistry.setOptionOverrides("@quartz-community/explorer", {
	sortFn: (a, b) => {
		// Appendix pages go to the bottom of the explorer
		const aApp = /^appendix/i.test(a.displayName) ? 1 : 0
		const bApp = /^appendix/i.test(b.displayName) ? 1 : 0
		if (aApp !== bApp) return aApp - bApp
		return a.displayName.localeCompare(b.displayName, undefined, {
			numeric: true,
			sensitivity: "base",
		})
	},
})

componentRegistry.setOptionOverrides("@quartz-community/folder-page", {
	sort: (a, b) => {
		const aTitle = a.frontmatter?.title ?? ""
		const bTitle = b.frontmatter?.title ?? ""
		// Appendix pages go to the bottom of the folder listing
		const aApp = /^appendix/i.test(aTitle) ? 1 : 0
		const bApp = /^appendix/i.test(bTitle) ? 1 : 0
		if (aApp !== bApp) return aApp - bApp
		return aTitle.localeCompare(bTitle, undefined, {
			numeric: true,
			sensitivity: "base",
		})
	},
})

const config = await loadQuartzConfig()
export default config
export const layout = await loadQuartzLayout()
