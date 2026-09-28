import { availableParallelism } from "node:os"
import { Worker } from "node:worker_threads"

const configuredSize = Number.parseInt(process.env.RNA_WORKER_COUNT ?? "", 10)
const configuredMaxQueueSize = Number.parseInt(process.env.RNA_MAX_QUEUE_SIZE ?? "", 10)
const configuredJobTimeout = Number.parseInt(process.env.RNA_JOB_TIMEOUT_MS ?? "", 10)

const poolSize =
    Number.isInteger(configuredSize) && configuredSize > 0 ? configuredSize : Math.max(1, availableParallelism() - 1)
const maxQueueSize =
    Number.isInteger(configuredMaxQueueSize) && configuredMaxQueueSize > 0 ? configuredMaxQueueSize : 100
const jobTimeoutMs = Number.isInteger(configuredJobTimeout) && configuredJobTimeout > 0 ? configuredJobTimeout : 30_000

function createPoolError(message, code, statusCode) {
    const error = new Error(message)
    error.code = code
    error.statusCode = statusCode
    return error
}

function errorFromWorker(errorData) {
    const code = errorData?.code ?? "RNA_JOB_FAILED"
    const statusCode = code === "RNA_JOB_TIMEOUT" ? 504 : code === "RNA_JOB_CANCELLED" ? 499 : 500

    return createPoolError(errorData?.message ?? "RNAfold job failed", code, statusCode)
}

class RnaFoldWorkerPool {
    constructor(size) {
        this.size = size
        this.workers = []
        this.queue = []
        this.nextJobId = 1
        this.closing = false

        for (let index = 0; index < size; index += 1) {
            this.createWorker()
        }
    }

    createWorker() {
        const worker = new Worker(new URL("./workers/rna-fold.worker.js", import.meta.url), {
            workerData: { projectRoot: import.meta.dirname }
        })

        const slot = {
            worker,
            job: null,
            failed: false,
            shutdownResolve: null
        }

        this.workers.push(slot)

        worker.on("message", (message) => {
            if (message.type === "shutdownComplete") {
                slot.shutdownResolve?.()
                return
            }

            const job = slot.job

            if (!job || message.jobId !== job.id) return

            slot.job = null
            job.slot = null

            if (!job.settled) {
                if (message.success) {
                    this.resolveJob(job, message.result)
                } else {
                    this.rejectJob(job, errorFromWorker(message.error))
                }
            }

            this.dispatch()
        })

        worker.on("error", (error) => {
            slot.failed = true

            if (slot.job) {
                this.rejectJob(slot.job, error)
                slot.job = null
            }
        })

        worker.on("exit", (code) => {
            this.workers = this.workers.filter((item) => item !== slot)

            if (slot.job) {
                this.rejectJob(slot.job, new Error(`RNA worker exited with code ${code}`))
                slot.job = null
            }

            if (!this.closing) {
                this.createWorker()
                this.dispatch()
            }
        })

        return slot
    }

    resolveJob(job, result) {
        if (job.settled) return

        job.settled = true
        job.cleanupAbortListener?.()
        job.resolve(result)
    }

    rejectJob(job, error) {
        if (job.settled) return

        job.settled = true
        job.cleanupAbortListener?.()
        job.reject(error)
    }

    cancelJob(job) {
        if (job.settled) return

        const error = createPoolError("RNAfold job was cancelled", "RNA_JOB_CANCELLED", 499)
        const queueIndex = this.queue.indexOf(job)

        if (queueIndex !== -1) {
            this.queue.splice(queueIndex, 1)
            this.rejectJob(job, error)
            this.dispatch()
            return
        }

        if (job.slot) {
            job.slot.worker.postMessage({
                type: "cancel",
                jobId: job.id
            })
        }

        this.rejectJob(job, error)
    }

    run(sequence, { signal } = {}) {
        if (this.closing) {
            return Promise.reject(createPoolError("RNA worker pool is closing", "RNA_POOL_CLOSING", 503))
        }

        if (signal?.aborted) {
            return Promise.reject(createPoolError("RNAfold job was cancelled", "RNA_JOB_CANCELLED", 499))
        }

        const hasAvailableWorker = this.workers.some((slot) => !slot.job && !slot.failed)

        if (!hasAvailableWorker && this.queue.length >= maxQueueSize) {
            return Promise.reject(createPoolError("RNAfold queue is full; try again later", "RNA_QUEUE_FULL", 503))
        }

        return new Promise((resolve, reject) => {
            const job = {
                id: this.nextJobId,
                sequence,
                resolve,
                reject,
                settled: false,
                slot: null,
                cleanupAbortListener: null
            }

            if (signal) {
                const handleAbort = () => this.cancelJob(job)
                signal.addEventListener("abort", handleAbort, { once: true })
                job.cleanupAbortListener = () => {
                    signal.removeEventListener("abort", handleAbort)
                }
            }

            this.queue.push(job)

            this.nextJobId += 1
            this.dispatch()
        })
    }

    dispatch() {
        for (const slot of this.workers) {
            if (this.queue.length === 0) return
            if (slot.job || slot.failed) continue

            const job = this.queue.shift()
            slot.job = job
            job.slot = slot

            slot.worker.postMessage({
                type: "run",
                jobId: job.id,
                sequence: job.sequence,
                timeoutMs: jobTimeoutMs
            })
        }
    }

    async close() {
        this.closing = true

        const error = new Error("RNA worker pool was closed")

        for (const job of this.queue.splice(0)) {
            this.rejectJob(job, error)
        }

        await Promise.all(
            this.workers.map(async (slot) => {
                if (slot.job) {
                    this.rejectJob(slot.job, error)
                }

                await new Promise((resolve) => {
                    const timeout = setTimeout(resolve, 2_000)

                    slot.shutdownResolve = () => {
                        clearTimeout(timeout)
                        resolve()
                    }

                    try {
                        slot.worker.postMessage({ type: "shutdown" })
                    } catch {
                        slot.shutdownResolve()
                    }
                })

                slot.shutdownResolve = null
                await slot.worker.terminate()
            })
        )

        this.workers = []
    }
}

const pool = new RnaFoldWorkerPool(poolSize)

export default async function runRnaFold(sequence, options) {
    return pool.run(sequence, options)
}

export async function closeRnaFoldPool() {
    await pool.close()
}

export function getRnaFoldPoolStatus() {
    return {
        size: pool.size,
        maxQueueSize,
        jobTimeoutMs,
        busy: pool.workers.filter((slot) => slot.job).length,
        queued: pool.queue.length
    }
}
