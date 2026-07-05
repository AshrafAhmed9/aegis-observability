import { useEffect, useRef, useState } from 'react'

const MILESTONES = [
  { id: 'fleet', label: 'Fleet started' },
  { id: 'fault', label: 'Fault injected' },
  { id: 'prediction', label: 'Prediction fired' },
  { id: 'breach', label: 'Breach occurred' },
  { id: 'incident', label: 'Incident root-caused' },
  { id: 'postmortem', label: 'Postmortem generated' },
]

export default function DemoTimeline({ state, simStatus }) {
  const [hits, setHits] = useState({})
  const faultTarget = useRef(null)

  useEffect(() => {
    if (!state) return
    const now = Date.now()
    setHits((prev) => {
      const next = { ...prev }

      if (!next.fleet && (state.services?.length ?? 0) > 0) {
        next.fleet = now
      }
      if (simStatus?.fault) {
        if (!next.fault) next.fault = now
        // Keep this in sync with whichever fault is CURRENTLY active, not
        // just the first one ever seen — otherwise injecting a second fault
        // without clicking Reset leaves this watching the wrong service.
        faultTarget.current = simStatus.fault.startsWith('redis') ? 'redis-cache'
          : simStatus.fault.startsWith('queue') ? 'payment-worker' : 'postgres-db'
      }
      if (!next.prediction && next.fault && (state.predictions?.length ?? 0) > 0) {
        next.prediction = now
      }
      if (!next.breach && next.fault && faultTarget.current) {
        const target = state.services?.find((s) => s.name === faultTarget.current)
        if (target && (target.status === 'ERROR' || target.status === 'CRITICAL')) {
          next.breach = now
        }
      }
      // incidents[] is newest-first (appendleft on the backend); only the
      // most recent entry counts, and only if it happened after this fault
      // was injected — otherwise a stale incident from an earlier demo run
      // sitting in the ring buffer would falsely check this off instantly.
      const latestIncident = state.incidents?.[0]
      if (!next.incident && next.fault && latestIncident?.root_cause_service
          && latestIncident.ts * 1000 >= next.fault) {
        next.incident = now
        next.postmortem = now
      }
      return next
    })
  }, [state, simStatus])

  function reset() {
    setHits({})
    faultTarget.current = null
  }

  const leadSeconds = hits.prediction && hits.breach
    ? Math.round((hits.breach - hits.prediction) / 1000)
    : null

  return (
    <section className="panel demo-timeline">
      <div className="demo-timeline__head">
        <h2 className="panel__title">Demo Timeline</h2>
        <button className="demo-timeline__reset" onClick={reset}>Reset</button>
      </div>
      <ol className="demo-timeline__list">
        {MILESTONES.map((m) => {
          const ts = hits[m.id]
          return (
            <li key={m.id} className={`demo-timeline__row ${ts ? 'demo-timeline__row--done' : ''}`}>
              <span className="demo-timeline__check">{ts ? '✓' : '○'}</span>
              <span className="demo-timeline__label">{m.label}</span>
              <span className="demo-timeline__time">
                {ts ? new Date(ts).toLocaleTimeString() : '—'}
              </span>
            </li>
          )
        })}
      </ol>
      {leadSeconds !== null && (
        <div className="demo-timeline__lead">
          {leadSeconds > 5 ? (
            <>Predicted <strong>{leadSeconds}s</strong> before failure</>
          ) : (
            <>Detected reactively — this fault gave no advance warning</>
          )}
        </div>
      )}
    </section>
  )
}
