function createCsvToNdjsonStream({ delimiter = ",", headers = true } = {}) {
    const decoder = new TextDecoder()
    const encoder = new TextEncoder()

    let field = ""
    let row = []
    let headerRow = null

    let inQuotes = false
    let quotePending = false
    let pendingCR = false
    let firstChunk = true

    function finishField() {
        row.push(field)
        field = ""
    }

    function finishRow(controller) {
        finishField()

        // Ignore empty lines
        if (row.length === 1 && row[0] === "") {
            row = []
            return
        }

        let value

        if (headers) {
            if (headerRow === null) {
                headerRow = row
                row = []
                return
            }

            value = {}

            for (let i = 0; i < headerRow.length; i++) {
                value[headerRow[i]] = row[i] ?? ""
            }
        } else {
            value = row
        }

        controller.enqueue(encoder.encode(JSON.stringify(value) + "\n"))

        row = []
    }

    function parseText(text, controller) {
        if (firstChunk) {
            firstChunk = false

            // Remove UTF-8 BOM if present
            if (text.charCodeAt(0) === 0xfeff) {
                text = text.slice(1)
            }
        }

        for (let i = 0; i < text.length; i++) {
            const char = text[i]

            // Previous chunk/character ended with CR.
            // Ignore LF in CRLF.
            if (pendingCR) {
                pendingCR = false

                if (char === "\n") {
                    continue
                }
            }

            if (inQuotes) {
                if (quotePending) {
                    if (char === '"') {
                        // ""
                        field += '"'
                        quotePending = false
                        continue
                    }

                    // Previous " closed the quoted field
                    inQuotes = false
                    quotePending = false

                    // Process current char normally below
                } else {
                    if (char === '"') {
                        quotePending = true
                    } else {
                        field += char
                    }

                    continue
                }
            }

            if (char === delimiter) {
                finishField()
            } else if (char === "\n") {
                finishRow(controller)
            } else if (char === "\r") {
                finishRow(controller)
                pendingCR = true
            } else if (char === '"' && field.length === 0) {
                inQuotes = true
            } else {
                field += char
            }
        }
    }

    return new TransformStream({
        transform(chunk, controller) {
            // chunk is Uint8Array from file.stream()
            const text = decoder.decode(chunk, {
                stream: true
            })

            parseText(text, controller)
        },

        flush(controller) {
            // Flush any incomplete UTF-8 character
            const text = decoder.decode()

            if (text) {
                parseText(text, controller)
            }

            if (quotePending) {
                // Closing quote at EOF
                quotePending = false
                inQuotes = false
            }

            if (inQuotes) {
                throw new Error("Invalid CSV: file ended inside quoted field")
            }

            if (field.length || row.length) {
                finishRow(controller)
            }
        }
    })
}

async function emitNdjsonFile(file) {
    const reader = file
        .stream()
        .pipeThrough(new TextDecoderStream())
        .getReader()

    let buffer = ""
    let lineNumber = 0

    function emitLine(line) {
        lineNumber += 1

        const trimmedLine = line.trim()
        if (!trimmedLine) return

        try {
            self.postMessage(JSON.parse(trimmedLine))
        } catch (error) {
            throw new Error(`Invalid NDJSON on line ${lineNumber}: ${error.message}`)
        }
    }

    try {
        while (true) {
            const { value, done } = await reader.read()

            if (done) break

            buffer += value

            const lines = buffer.split("\n")
            buffer = lines.pop()

            for (const line of lines) {
                emitLine(line)
            }
        }

        if (buffer) {
            emitLine(buffer)
        }
    } finally {
        reader.releaseLock()
    }
}

self.onmessage = async (event) => {
    const { file, filename, new_calculation = false } = event.data

    if (!new_calculation) {
        try {
            await emitNdjsonFile(file)
            self.postMessage({ type: "loaded" })
        } catch (error) {
            self.postMessage({
                type: "error",
                name: error.name,
                message: error.message,
                stack: error.stack
            })
        } finally {
            self.close()
        }

        return
    }

    let events
    let writable
    let writeChain = Promise.resolve()

    try {
        const root = await navigator.storage.getDirectory()
        const handle = await root.getFileHandle(filename ?? `data-${Date.now()}.ndjson`, { create: true })

        writable = await handle.createWritable()

        const id = crypto.randomUUID()
        events = new EventSource(`/runs/${id}/events`)

        const { promise: ready, resolve: resolveReady, reject: rejectReady } = Promise.withResolvers()

        const { promise: finished, resolve: resolveFinished, reject: rejectFinished } = Promise.withResolvers()

        let isReady = false
        let isFinished = false
        let writeFailed = false

        events.onmessage = (messageEvent) => {
            try {
                const message = JSON.parse(messageEvent.data)

                self.postMessage(message)

                writeChain = writeChain.then(() => writable.write(`${messageEvent.data}\n`))
                void writeChain.catch((error) => {
                    if (writeFailed) return
                    writeFailed = true
                    events.close()
                    rejectReady(error)
                    rejectFinished(error)
                })

                if (message.type === "ready") {
                    isReady = true
                    resolveReady()
                }

                if (message.type === "done" || message.type === "fatal" || message.type === "cancelled") {
                    isFinished = true
                    events.close()
                    writeChain.then(
                        () => resolveFinished(message),
                        (error) => rejectFinished(error)
                    )
                }
            } catch (error) {
                events.close()
                rejectReady(error)
                rejectFinished(error)
            }
        }

        events.onerror = () => {
            if (isFinished) return

            const error = new Error("The result event stream disconnected")

            if (!isReady) rejectReady(error)
            rejectFinished(error)
            events.close()
        }

        await ready

        const stream = file.stream().pipeThrough(createCsvToNdjsonStream())
        const upload = fetch(`/runs/${id}/upload`, {
            method: "POST",
            headers: {
                "Content-Type": "application/ndjson"
            },
            body: stream,
            duplex: "half"
        })

        const [uploadResponse] = await Promise.all([upload, finished])

        if (!uploadResponse.ok) {
            const message = await uploadResponse.text()
            throw new Error(message || `Upload failed with ${uploadResponse.status}`)
        }
    } catch (error) {
        console.error("WORKER FETCH ERROR", error)

        self.postMessage({
            type: "error",
            name: error.name,
            message: error.message,
            stack: error.stack
        })
    } finally {
        events?.close()

        try {
            await writeChain
            await writable?.close()
        } catch {
            try {
                await writable?.abort()
            } catch {
                // Nothing else to clean up.
            }
        }

        self.close()
    }
}
