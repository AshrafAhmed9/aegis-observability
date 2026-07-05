import { useEffect, useRef, useState } from 'react'

export function usePolling(intervalMs) {
  const [state, setState] = useState(null)
  const [error, setError] = useState(null)
  const timerRef = useRef(null)

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()

    async function fetchState() {
      try {
        const res = await fetch('/dashboard/state', { signal: controller.signal })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        if (!cancelled) {
          setState(data)
          setError(null)
        }
      } catch (err) {
        if (!cancelled && err.name !== 'AbortError') {
          setError(err.message)
        }
      }
    }

    fetchState()
    timerRef.current = setInterval(fetchState, intervalMs)

    return () => {
      cancelled = true
      controller.abort()
      clearInterval(timerRef.current)
    }
  }, [intervalMs])

  return { state, error }
}

export async function simulatorStart() {
  return fetch('/simulator/start', { method: 'POST' }).then((r) => r.json())
}

export async function simulatorStop() {
  return fetch('/simulator/stop', { method: 'POST' }).then((r) => r.json())
}

export async function simulatorInject(fault) {
  return fetch('/simulator/inject', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ fault }),
  }).then((r) => r.json())
}

export async function simulatorStatus() {
  return fetch('/simulator/status').then((r) => r.json())
}

export async function infraStatus() {
  return fetch('/infra/status').then((r) => r.json())
}

export async function kafkaStats() {
  return fetch('/kafka/stats').then((r) => r.json())
}

export async function kafkaReplay(scenario, rate) {
  const res = await fetch('/kafka/replay', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scenario, rate }),
  })
  const body = await res.json()
  if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`)
  return body
}

export async function warRoomList() {
  return fetch('/war-room').then((r) => r.json())
}

export async function warRoomFile(name) {
  return fetch(`/war-room/${name}`).then((r) => r.json())
}

export async function evalScorecard() {
  return fetch('/eval/scorecard').then((r) => r.json())
}

export async function listScenarios() {
  return fetch('/scenarios').then((r) => r.json())
}

export function usePrevious(value) {
  const ref = useRef(null)
  useEffect(() => {
    ref.current = value
  }, [value])
  return ref.current
}

// Generic interval poller for the platform endpoints.
export function useIntervalFetch(fn, intervalMs, active = true) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (!active) return undefined
    let cancelled = false
    async function tick() {
      try {
        const result = await fn()
        if (!cancelled) setData(result)
      } catch {
        if (!cancelled) setData(null)
      }
    }
    tick()
    const id = setInterval(tick, intervalMs)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [intervalMs, active])

  return data
}
