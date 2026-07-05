import { useEffect, useState } from 'react'

function Countdown({ etaSeconds, predictedAt, watermark }) {
  const [now, setNow] = useState(() => Date.now() / 1000)

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now() / 1000), 1000)
    return () => clearInterval(id)
  }, [])

  if (etaSeconds == null) return null
  const elapsedSincePoll = now - (watermark ?? predictedAt)
  const remaining = Math.max(0, etaSeconds - elapsedSincePoll)
  const mins = Math.floor(remaining / 60)
  const secs = Math.floor(remaining % 60)
  return (
    <span className="prediction-card__countdown">
      {mins}m {secs.toString().padStart(2, '0')}s
    </span>
  )
}

function severityClass(sev) {
  if (sev === 'CRITICAL') return 'sev-critical'
  if (sev === 'ERROR') return 'sev-serious'
  if (sev === 'WARNING') return 'sev-warning'
  return 'sev-good'
}

export default function PredictionsPanel({ predictions, watermark }) {
  if (!predictions || predictions.length === 0) {
    return (
      <section className="panel predictions-panel">
        <h2 className="panel__title">Active Predictions</h2>
        <p className="predictions-panel__empty">No active predictions — all systems nominal.</p>
      </section>
    )
  }

  return (
    <section className="panel predictions-panel">
      <h2 className="panel__title">Active Predictions</h2>
      <div className="predictions-panel__list">
        {predictions.map((p, i) => {
          const pct = p.threshold
            ? Math.min(100, (p.current_value / p.threshold) * 100)
            : null
          return (
            <div key={i} className={`prediction-card ${severityClass(p.severity)}`}>
              <div className="prediction-card__head">
                <span className="prediction-card__kind">{p.kind.replace('_', ' ')}</span>
                <span className="prediction-card__confidence">conf {Math.round(p.confidence * 100)}%</span>
              </div>
              <p className="prediction-card__summary">{p.summary}</p>
              {pct !== null && (
                <div className="meter">
                  <div className="meter__track">
                    <div className="meter__fill" style={{ width: `${pct}%` }} />
                  </div>
                </div>
              )}
              {p.eta_seconds != null && (
                <Countdown etaSeconds={p.eta_seconds} predictedAt={p.predicted_at} watermark={watermark} />
              )}
            </div>
          )
        })}
      </div>
    </section>
  )
}
