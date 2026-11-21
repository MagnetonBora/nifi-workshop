import { loadQuartzConfig, loadQuartzLayout } from "./quartz/plugins/loader/config-loader"
import { componentRegistry } from "./quartz/components/registry"

componentRegistry.setOptionOverrides("@quartz-community/explorer", {
	sortFn: (a, b) => {
		// Final lab goes after the numbered labs; appendix pages go to the very bottom.
		// No helper functions here: sortFn is serialized and runs in the browser.
		if (a.isFolder && b.isFolder) {
			// Fixed order of the top-level folders; unknown folders go after them
			const order = ["materials", "labs", "solutions"]
			const aPos = order.indexOf(a.slugSegment) === -1 ? order.length : order.indexOf(a.slugSegment)
			const bPos = order.indexOf(b.slugSegment) === -1 ? order.length : order.indexOf(b.slugSegment)
			if (aPos !== bPos) return aPos - bPos
		}
		const aRank = /^appendix/i.test(a.displayName) ? 2 : /^final/i.test(a.displayName) ? 1 : 0
		const bRank = /^appendix/i.test(b.displayName) ? 2 : /^final/i.test(b.displayName) ? 1 : 0
		if (aRank !== bRank) return aRank - bRank
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
		// Final lab goes after the numbered labs; appendix pages go to the very bottom
		const aRank = /^appendix/i.test(aTitle) ? 2 : /^final/i.test(aTitle) ? 1 : 0
		const bRank = /^appendix/i.test(bTitle) ? 2 : /^final/i.test(bTitle) ? 1 : 0
		if (aRank !== bRank) return aRank - bRank
		return aTitle.localeCompare(bTitle, undefined, {
			numeric: true,
			sensitivity: "base",
		})
	},
})

const config = await loadQuartzConfig()
export default config
export const layout = await loadQuartzLayout()
