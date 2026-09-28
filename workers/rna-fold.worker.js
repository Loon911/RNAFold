import path from "node:path"
import { spawn } from "node:child_process"
import { createInterface } from "node:readline"
import { parentPort, workerData } from "node:worker_threads"

const pythonPath = path.join(workerData.projectRoot, ".venv", "bin", "python")

const pythonCode = `
import json
import sys
import RNA

for line in sys.stdin:
    job_id = None

    try:
        request = json.loads(line)
        job_id = request.get("jobId")
        sequence = request["sequence"].strip().upper()
        structure, energy = RNA.fold(sequence)

        response = {
            "jobId": job_id,
            "success": True,
            "sequence": sequence,
            "structure": structure,
            "energy": energy,
        }
    except Exception as error:
        response = {
            "jobId": job_id,
            "success": False,
            "error": str(error),
        }

    print(json.dumps(response, separators=(",", ":")), flush=True)
`

let python = null
let activeJob = null
let shuttingDown = false
let restartTimer = null
let forceKillTimer = null
let shutdownReported = false

function createJobError(message, code = "RNA_JOB_FAILED") {
    const error = new Error(message)
    error.code = code
    return error
}

function buildResult({ sequence, structure, energy }) {
    if (!/^[ACGU]+$/.test(sequence)) {
        throw new Error("RNAfold returned an invalid RNA sequence")
    }

    if (!/^[().]+$/.test(structure)) {
        throw new Error("RNAfold returned an invalid structure")
    }

    if (structure.length !== sequence.length) {
        throw new Error(
            `RNAfold returned a structure of length ${structure.length} ` +
                `for a sequence of length ${sequence.length}`
        )
    }

    if (!Number.isFinite(energy)) {
        throw new Error("RNAfold returned an invalid energy value")
    }

    const stack = []
    const basePairs = []

    for (let index = 0; index < structure.length; index += 1) {
        const symbol = structure[index]

        if (symbol === "(") {
            stack.push(index)
            continue
        }

        if (symbol === ")") {
            const openingIndex = stack.pop()

            if (openingIndex === undefined) {
                throw new Error(`RNAfold returned an unmatched ')' at position ${index + 1}`)
            }

            basePairs.push({
                from: openingIndex + 1,
                to: index + 1,
                fromBase: sequence[openingIndex],
                toBase: sequence[index]
            })
        }
    }

    if (stack.length > 0) {
        throw new Error(`RNAfold returned an unmatched '(' at position ${stack.at(-1) + 1}`)
    }

    basePairs.sort((left, right) => left.from - right.from)

    const pairedNucleotideCount = basePairs.length * 2

    return {
        sequence,
        length: sequence.length,
        structure,
        energy,
        energyUnit: "kcal/mol",
        basePairs,
        basePairCount: basePairs.length,
        pairedNucleotideCount,
        unpairedNucleotideCount: sequence.length - pairedNucleotideCount
    }
}

function reportShutdownComplete() {
    if (shutdownReported) return
    shutdownReported = true
    parentPort.postMessage({ type: "shutdownComplete" })
}

function settleActiveJob(error, result) {
    const job = activeJob

    if (!job) return

    activeJob = null
    clearTimeout(job.timeout)

    if (error) {
        job.reject(error)
    } else {
        job.resolve(result)
    }
}

function terminatePython(error) {
    if (activeJob && error && !activeJob.terminationError) {
        activeJob.terminationError = error
    }

    if (!python || python.exitCode !== null || python.signalCode !== null) return

    python.kill("SIGTERM")
    clearTimeout(forceKillTimer)
    forceKillTimer = setTimeout(() => {
        if (python && python.exitCode === null && python.signalCode === null) {
            python.kill("SIGKILL")
        }
    }, 1_000)
}

function handlePythonMessage(message) {
    if (!activeJob || message.jobId !== activeJob.jobId) return

    if (!message.success) {
        settleActiveJob(createJobError(message.error || "RNAfold failed"))
        return
    }

    try {
        settleActiveJob(null, buildResult(message))
    } catch (error) {
        settleActiveJob(error)
    }
}

function startPython() {
    if (shuttingDown || python) return

    const child = spawn(pythonPath, ["-u", "-c", pythonCode], {
        stdio: ["pipe", "pipe", "pipe"]
    })
    const lines = createInterface({ input: child.stdout })

    python = child

    lines.on("line", (line) => {
        try {
            handlePythonMessage(JSON.parse(line))
        } catch (error) {
            terminatePython(createJobError(`Invalid response from Python: ${error.message}`))
        }
    })

    child.stderr.setEncoding("utf8")
    child.stderr.on("data", (chunk) => {
        if (activeJob) activeJob.stderr += chunk
    })

    child.on("error", (error) => {
        if (python !== child) return

        python = null
        clearTimeout(forceKillTimer)
        lines.close()
        settleActiveJob(error)

        if (shuttingDown) {
            reportShutdownComplete()
        } else {
            restartTimer = setTimeout(startPython, 250)
        }
    })

    child.on("close", (code, signal) => {
        if (python !== child) return

        python = null
        clearTimeout(forceKillTimer)
        lines.close()

        if (activeJob) {
            const error =
                activeJob.terminationError ??
                createJobError(
                    activeJob.stderr.trim() ||
                        `Persistent Python worker exited with code ${code}, signal ${signal}`
                )

            settleActiveJob(error)
        }

        if (shuttingDown) {
            reportShutdownComplete()
        } else {
            startPython()
        }
    })
}

function executeRnaFold(sequence, jobId, timeoutMs) {
    if (!python || python.exitCode !== null || python.signalCode !== null) {
        return Promise.reject(createJobError("Persistent Python worker is unavailable"))
    }

    if (activeJob) {
        return Promise.reject(createJobError("Persistent Python worker is already busy"))
    }

    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            terminatePython(
                createJobError(
                    `RNAfold exceeded the ${timeoutMs}ms execution limit`,
                    "RNA_JOB_TIMEOUT"
                )
            )
        }, timeoutMs)

        activeJob = {
            jobId,
            resolve,
            reject,
            timeout,
            terminationError: null,
            stderr: ""
        }

        python.stdin.write(`${JSON.stringify({ jobId, sequence })}\n`, (error) => {
            if (error && activeJob?.jobId === jobId) terminatePython(error)
        })
    })
}

startPython()

parentPort.on("message", async (message) => {
    if (message.type === "cancel") {
        if (activeJob?.jobId === message.jobId) {
            terminatePython(createJobError("RNAfold job was cancelled", "RNA_JOB_CANCELLED"))
        }

        return
    }

    if (message.type === "shutdown") {
        shuttingDown = true
        clearTimeout(restartTimer)

        if (python) {
            terminatePython(
                createJobError("RNAfold worker is shutting down", "RNA_JOB_CANCELLED")
            )
        } else {
            reportShutdownComplete()
        }

        return
    }

    if (message.type !== "run") return

    const { jobId, sequence, timeoutMs } = message

    try {
        const result = await executeRnaFold(sequence, jobId, timeoutMs)

        parentPort.postMessage({
            jobId,
            success: true,
            result
        })
    } catch (error) {
        parentPort.postMessage({
            jobId,
            success: false,
            error: {
                code: error.code ?? "RNA_JOB_FAILED",
                message: error.message
            }
        })
    }
})
