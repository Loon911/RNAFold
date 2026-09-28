import { Virtualizer, elementScroll, observeElementOffset, observeElementRect } from "/js/library/tanstack-virtual.js"

class workspace {
    static main = document.querySelector("main")
    static list = document.querySelector("#RNA_list")
    static template = document.querySelector("#template_row")
    static active = null
    static renderScheduled = false

    static scheduleRender = () => {
        if (this.renderScheduled) return

        this.renderScheduled = true

        requestAnimationFrame(() => {
            this.renderScheduled = false
            this.active?.renderVisibleRows()
        })
    }

    static setActive(nextWorkspace) {
        if (this.active === nextWorkspace) return

        this.active?.deactivate()
        this.active = nextWorkspace
        this.active?.activate()
    }

    constructor({ file, filename = null, new_calculation = false } = {}) {
        if (!file) return alert("ERROR")

        this.file = file
        this.new_calculation = new_calculation

        if (filename) {
            this.filename = filename
        } else {
            const timestamp = new Date().toISOString().replace(/[:.]/g, "-")
            this.filename = `data-${timestamp}.ndjson`
        }

        this.virtualizer = new Virtualizer(this.getVirtualizerOptions())
        this.cleanupVirtualizer = this.virtualizer._didMount()
        this.virtualizer._willUpdate()

        this.start()
    }

    rna_folds = []

    getScrollElement = () => {
        if (this.constructor.active !== this) return null

        return this.constructor.main
    }

    estimateSize() {
        return 50
    }

    getItemKey(index) {
        return index
    }

    getVirtualizerOptions() {
        return {
            count: this.rna_folds.length,
            getScrollElement: this.getScrollElement,
            estimateSize: this.estimateSize,
            getItemKey: this.getItemKey,
            gap: 8,
            overscan: 8,
            observeElementRect,
            observeElementOffset,
            scrollToFn: elementScroll,
            onChange: this.constructor.scheduleRender
        }
    }

    renderedRows = new Map()
    scrollTop = 0

    renderVisibleRows() {
        if (this.constructor.active !== this) return

        this.constructor.list.style.height = `${this.virtualizer.getTotalSize()}px`

        const requiredKeys = new Set()

        for (const virtualRow of this.virtualizer.getVirtualItems()) {
            const data = this.rna_folds[virtualRow.index]
            if (!data?.result) continue

            requiredKeys.add(virtualRow.key)

            let row = this.renderedRows.get(virtualRow.key)

            if (!row) {
                row = this.constructor.template.content.firstElementChild.cloneNode(true)
                // row = document.createElement("div")
                // row.className = "row"
                this.renderedRows.set(virtualRow.key, row)
                this.constructor.list.append(row)
            }

            if (row.dataset.index !== String(virtualRow.index)) {
                row.dataset.index = virtualRow.index
                row.querySelector(".RNA_id").textContent = data.id
                row.querySelector(".RNA_sequence").textContent = data.result.sequence
                row.querySelector(".RNA_structure").textContent = data.result.structure
                row.querySelector(".RNA_energy").textContent = `Energy: ${data.result.energy} ${data.result.energyUnit}`
                row.querySelector(".RNA_basePairCount").textContent = `Base pairs: ${data.result.basePairCount}`
                row.querySelector(".RNA_pairedNucleotideCount").textContent =
                    `Paired nucleotides: ${data.result.pairedNucleotideCount}`
                row.querySelector(".RNA_unpairedNucleotideCount").textContent =
                    `Unpaired nucleotides: ${data.result.unpairedNucleotideCount}`
                row.querySelector(".RNA_length").textContent = `Length: ${data.result.length} nt`
            }

            row.style.transform = `translateY(${virtualRow.start}px)`
            this.virtualizer.measureElement(row)
        }

        for (const [key, row] of this.renderedRows) {
            if (requiredKeys.has(key)) continue

            row.remove()
            this.renderedRows.delete(key)
        }

        this.constructor.list.style.height = `${this.virtualizer.getTotalSize()}px`
    }

    activate() {
        this.virtualizer.setOptions(this.getVirtualizerOptions())
        this.constructor.list.style.height = `${this.virtualizer.getTotalSize()}px`
        this.constructor.main.scrollTop = this.scrollTop
        this.virtualizer._willUpdate()
        this.constructor.scheduleRender()
    }

    deactivate() {
        this.scrollTop = this.constructor.main.scrollTop
        this.cleanupVirtualizer()

        for (const row of this.renderedRows.values()) {
            row.remove()
        }

        this.renderedRows.clear()
        this.constructor.list.style.height = "0px"
    }

    start() {
        this.worker = new Worker("/js/workers/upload-csv.js", {
            type: "module"
        })

        this.worker.onmessage = (event) => {
            if (event.data?.type !== "result") return

            console.log(event.data)
            this.rna_folds.push(event.data)
            this.virtualizer.setOptions(this.getVirtualizerOptions())

            if (this.constructor.active === this) {
                this.virtualizer._willUpdate()
                this.constructor.scheduleRender()
            }
        }

        this.worker.postMessage({
            file: this.file,
            filename: this.filename,
            new_calculation: this.new_calculation
        })
    }
}

const saves = document.querySelector("#saves")

function selectSave(saveElement) {
    saves.querySelector(".selected")?.classList.remove("selected")
    saveElement?.classList.add("selected")
}

function addSave({ name, handle = null, nextWorkspace = null }) {
    const save = document.createElement("div")
    save.textContent = name
    save.file_name = name
    save.nextWorkspace = nextWorkspace
    save.workspacePromise = null

    save.addEventListener("click", async () => {
        if (!save.nextWorkspace) {
            save.workspacePromise ??= handle.getFile().then((file) => {
                return new workspace({
                    file,
                    filename: name,
                    new_calculation: false
                })
            })

            try {
                save.nextWorkspace = await save.workspacePromise
            } finally {
                save.workspacePromise = null
            }
        }

        selectSave(save)
        workspace.setActive(save.nextWorkspace)
    })

    saves.append(save)
    return save
}

document.querySelector("#upload").addEventListener("click", async () => {
    const [fileHandle] = await window.showOpenFilePicker({
        startIn: "downloads",
        types: [
            {
                description: "CSV files",
                accept: {
                    "text/csv": [".csv"]
                }
            }
        ]
    })

    const file = await fileHandle.getFile()

    const nextWorkspace = new workspace({
        file,
        new_calculation: true
    })

    const save = addSave({
        name: nextWorkspace.filename,
        nextWorkspace
    })

    selectSave(save)
    workspace.setActive(nextWorkspace)
})

document.querySelector("#new_project").addEventListener("click", () => {
    workspace.setActive(null)
    selectSave(null)
})

async function get_files() {
    const root = await navigator.storage.getDirectory()

    for await (const [name, handle] of root.entries()) {
        if (handle.kind === "file") {
            addSave({ name, handle })
        }
    }
}

get_files()
