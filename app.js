import fs from "node:fs"
import path from "node:path"
import Fastify from "fastify"
import fastifyView from "@fastify/view"
import fastifyStatic from "@fastify/static"
import ejs from "ejs"
import split2 from "split2"

import runRnaFold, { closeRnaFoldPool, getRnaFoldPoolStatus } from "./run-rna-fold.js"

const fastify = Fastify({
    forceCloseConnections: true,
    logger: {
        transport: {
            target: "pino-pretty",
            options: {
                colorize: true,
                translateTime: "SYS:standard",
                ignore: "pid,hostname",
                singleLine: false
            }
        }
    },
    http2: true,
    https: {
        key: fs.readFileSync("./localhost-key.pem"),
        cert: fs.readFileSync("./localhost-cert.pem")
    }
})

fastify.addContentTypeParser("application/ndjson", (request, payload, done) => {
    done(null, payload.pipe(split2(JSON.parse)))
})

await fastify.register(fastifyView, {
    engine: { ejs },
    root: path.join(import.meta.dirname, "views")
})

await fastify.register(fastifyStatic, {
    root: path.join(import.meta.dirname, "public")
    // prefix: "/public/",
})

const runs = new Map()

function sendRunEvent(run, message) {
    const payload = `data: ${JSON.stringify(message)}\n\n`

    for (const response of run.responses) {
        if (!response.destroyed && !response.writableEnded) {
            response.write(payload)
        }
    }
}

function finishRun(run, message) {
    if (run.finished) return
    run.finished = true

    sendRunEvent(run, message)

    for (const response of run.responses) {
        response.end()
    }

    run.responses.clear()
    clearTimeout(run.expirationTimer)
    runs.delete(run.id)
}

function cancelRun(run, error) {
    if (run.finished) return

    run.controller.abort()
    run.uploadRequest?.destroy()
    finishRun(run, {
        type: "cancelled",
        error
    })
}

function createRun(id) {
    const run = {
        id,
        controller: new AbortController(),
        responses: new Set(),
        uploadRequest: null,
        uploadStarted: false,
        finished: false,
        expirationTimer: null
    }

    run.expirationTimer = setTimeout(() => {
        run.controller.abort()
        finishRun(run, {
            type: "fatal",
            error: "Run expired before completion"
        })
    }, 10 * 60 * 1000)
    run.expirationTimer.unref()

    runs.set(id, run)

    return run
}

fastify.get("/runs/:id/events", async (request, reply) => {
    const { id } = request.params

    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id)) {
        return reply.code(400).send({ error: "Invalid run ID" })
    }

    const run = runs.get(id) ?? createRun(id)

    reply.hijack()
    reply.raw.writeHead(200, {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache, no-transform",
        "x-content-type-options": "nosniff"
    })

    run.responses.add(reply.raw)
    reply.raw.write(`data: ${JSON.stringify({ type: "ready", id: run.id })}\n\n`)

    const heartbeat = setInterval(() => {
        if (!reply.raw.destroyed && !reply.raw.writableEnded) {
            reply.raw.write(": keep-alive\n\n")
        }
    }, 15_000)
    heartbeat.unref()

    let disconnected = false

    const handleDisconnect = () => {
        if (disconnected) return
        disconnected = true

        clearInterval(heartbeat)
        run.responses.delete(reply.raw)

        if (!run.finished && run.responses.size === 0) {
            cancelRun(run, "Result event stream disconnected")
        }
    }

    request.raw.on("aborted", handleDisconnect)
    request.raw.stream?.on("close", handleDisconnect)
    reply.raw.on("close", handleDisconnect)
})

fastify.post("/runs/:id/upload", async (request, reply) => {
    const run = runs.get(request.params.id)

    if (!run) {
        return reply.code(404).send({ error: "Run not found" })
    }

    if (run.uploadStarted) {
        return reply.code(409).send({ error: "Upload already started" })
    }

    run.uploadStarted = true
    run.uploadRequest = request.raw

    let received = 0
    let completed = 0
    let failed = 0
    const concurrency = getRnaFoldPoolStatus().size
    const inFlight = new Set()

    const processRow = async (row, rowNumber) => {
        const id = row.rna_id ?? row.id ?? rowNumber

        if (typeof row.sequence !== "string" || !row.sequence) {
            failed += 1
            sendRunEvent(run, {
                type: "error",
                id,
                rowNumber,
                error: "Row must contain a non-empty sequence"
            })
            return
        }

        try {
            const result = await runRnaFold(row.sequence, {
                signal: run.controller.signal
            })

            completed += 1
            sendRunEvent(run, {
                type: "result",
                id,
                rowNumber,
                result
            })
        } catch (error) {
            if (run.controller.signal.aborted) return

            failed += 1
            sendRunEvent(run, {
                type: "error",
                id,
                rowNumber,
                error: error.message,
                code: error.code ?? "RNA_JOB_FAILED"
            })
        }
    }

    try {
        for await (const row of request.body) {
            if (run.controller.signal.aborted) break

            received += 1

            const task = processRow(row, received)
            inFlight.add(task)
            task.then(
                () => inFlight.delete(task),
                () => inFlight.delete(task)
            )

            if (inFlight.size >= concurrency) {
                await Promise.race(inFlight)
            }
        }

        await Promise.all(inFlight)

        if (run.controller.signal.aborted) {
            finishRun(run, {
                type: "cancelled",
                received,
                completed,
                failed
            })
        } else {
            finishRun(run, {
                type: "done",
                received,
                completed,
                failed
            })
        }

        return { done: true, received, completed, failed }
    } catch (error) {
        if (run.controller.signal.aborted) {
            if (!request.raw.destroyed) {
                return reply.code(499).send({ error: "Run cancelled" })
            }

            return
        }

        run.controller.abort()
        await Promise.allSettled(inFlight)
        request.log.error(error)

        finishRun(run, {
            type: "fatal",
            error: "Invalid NDJSON request",
            message: error.message
        })

        return reply.code(400).send({ error: "Invalid NDJSON" })
    } finally {
        if (run.uploadRequest === request.raw) {
            run.uploadRequest = null
        }
    }
})

fastify.get("/", (request, reply) => {
    return reply.viewAsync("index.html", { title: "meow" })
})

fastify.listen(
    {
        port: 3002,
        host: "0.0.0.0"
    },
    (err, address) => {
        if (err) {
            fastify.log.error(err)
            process.exit(1)
        }

        fastify.log.info(`Server listening at ${address}`)
    }
)

let shuttingDown = false

async function shutdown(signal) {
    if (shuttingDown) {
        process.exit(1)
    }

    shuttingDown = true

    fastify.log.info({ signal }, "Shutting down")

    const forceExitTimer = setTimeout(() => {
        fastify.log.warn("Graceful shutdown timed out; forcing exit")
        process.exit(1)
    }, 5_000)

    for (const run of [...runs.values()]) {
        run.controller.abort()
        finishRun(run, {
            type: "cancelled",
            error: "Server is shutting down"
        })
        run.uploadRequest?.destroy()
    }

    try {
        await closeRnaFoldPool()
        await fastify.close()
        clearTimeout(forceExitTimer)
        process.exit(0)
    } catch (error) {
        fastify.log.error(error)
        process.exit(1)
    }
}

process.on("SIGINT", () => {
    void shutdown("SIGINT")
})

process.on("SIGTERM", () => {
    void shutdown("SIGTERM")
})
