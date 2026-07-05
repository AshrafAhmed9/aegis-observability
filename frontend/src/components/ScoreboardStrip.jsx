function fmtLead(v) {
  return v == null ? '—' : `${Math.round(v)}s`
}

export default function ScoreboardStrip({ scoreboard }) {
  if (!scoreboard) return null
  const { STAT, ML } = scoreboard
  return (
    <section className="panel scoreboard-strip">
      <h2 className="panel__title">Champion / Challenger Scoreboard</h2>
      <div className="scoreboard-strip__row">
        {[['STAT', STAT], ['ML', ML]].map(([label, s]) => (
          <div key={label} className={`scoreboard-strip__col scoreboard-strip__col--${label.toLowerCase()}`}>
            <div className="scoreboard-strip__source">{label}</div>
            <div className="scoreboard-strip__stats">
              <div><span className="scoreboard-strip__value">{s?.hits ?? 0}</span><span className="scoreboard-strip__label">hits</span></div>
              <div><span className="scoreboard-strip__value">{s?.false_alarms ?? 0}</span><span className="scoreboard-strip__label">false alarms</span></div>
              <div><span className="scoreboard-strip__value">{s?.first_to_fire ?? 0}</span><span className="scoreboard-strip__label">first to fire</span></div>
              <div><span className="scoreboard-strip__value">{fmtLead(s?.mean_lead_seconds)}</span><span className="scoreboard-strip__label">mean lead</span></div>
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
