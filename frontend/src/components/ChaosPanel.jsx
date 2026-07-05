import { useState } from 'react'
import { simulatorStart, simulatorStop, simulatorInject } from '../api.js'

const FAULTS = [
  { id: 'redis_connection_leak', label: 'Redis connection leak' },
  { id: 'queue_backlog', label: 'Queue backlog' },
  { id: 'deadlock_burst', label: 'Deadlock burst' },
]

export default function ChaosPanel({ simStatus, onChange }) {
  const [busy, setBusy] = useState(false)

  async function run(fn) {
    setBusy(true)
    try {
      const result = await fn()
      onChange?.(result)
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="panel chaos-panel">
      <h2 className="panel__title">Chaos Console</h2>
      <div className="chaos-panel__controls">
        <button disabled={busy} onClick={() => run(simulatorStart)}>Start fleet</button>
        <button disabled={busy} onClick={() => run(simulatorStop)}>Stop fleet</button>
      </div>
      <div className="chaos-panel__faults">
        {FAULTS.map((f) => (
          <button
            key={f.id}
            disabled={busy}
            onClick={() => run(() => simulatorInject(f.id))}
          >
            Inject: {f.label}
          </button>
        ))}
      </div>
      {simStatus && (
        <div className="chaos-panel__status">
          <span>running: {String(simStatus.running)}</span>
          {simStatus.fault && (
            <>
              <span>fault: {simStatus.fault}</span>
              <span>phase: {simStatus.phase}</span>
              <span>elapsed: {simStatus.elapsed_seconds}s</span>
            </>
          )}
        </div>
      )}
    </section>
  )
}
