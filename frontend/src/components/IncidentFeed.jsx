import { useState } from 'react'

function severityClass(sev) {
  if (sev === 'CRITICAL') return 'sev-critical'
  if (sev === 'HIGH' || sev === 'ERROR') return 'sev-serious'
  if (sev === 'MEDIUM' || sev === 'WARNING') return 'sev-warning'
  return 'sev-good'
}

export default function IncidentFeed({ incidents }) {
  const [expanded, setExpanded] = useState(null)

  return (
    <section className="panel">
      <h2 className="panel__title">Incident Feed</h2>
      {incidents.length === 0 ? (
        <p className="incident-feed__empty">No incidents yet.</p>
      ) : (
        <ul className="incident-feed">
          {incidents.map((inc, i) => {
            const isOpen = expanded === i
            const mlTop = inc.ml_ranking?.ranking?.[0]
            const agrees = mlTop && inc.root_cause_service && mlTop.service === inc.root_cause_service
            return (
              <li key={i} className={`incident-row ${severityClass(inc.overall_severity)}`}>
                <button type="button" className="incident-row__toggle" onClick={() => setExpanded(isOpen ? null : i)}>
                  <div className="incident-row__head">
                    <span className="incident-row__id">{inc.incident_id}</span>
                    <span className="incident-row__sev">{inc.overall_severity}</span>
                  </div>
                  <p className="incident-row__title">{inc.title}</p>
                  <div className="incident-row__meta">
                    {inc.root_cause_service && (
                      <span>root: {inc.root_cause_service} ({inc.root_cause_class})</span>
                    )}
                    <span>blast: {inc.blast_radius_percentage.toFixed(0)}%</span>
                    {mlTop && (
                      <span className={`incident-row__agreement ${agrees ? 'incident-row__agreement--agree' : 'incident-row__agreement--disagree'}`}>
                        ML {agrees ? 'agrees' : 'disagrees'}
                      </span>
                    )}
                  </div>
                </button>
                {isOpen && (
                  <div className="incident-row__detail">
                    {inc.ml_ranking && (
                      <div className="incident-row__ranking">
                        <div className="incident-row__detail-label">ML root-cause ranking (v{inc.ml_ranking.version})</div>
                        <ul>
                          {inc.ml_ranking.ranking.map((r) => (
                            <li key={r.service}>{r.service}: {(r.ml_proba * 100).toFixed(1)}%</li>
                          ))}
                        </ul>
                      </div>
                    )}
                    {inc.similar_incidents?.length > 0 && (
                      <div className="incident-row__similar">
                        <div className="incident-row__detail-label">Similar past incidents</div>
                        <ul>
                          {inc.similar_incidents.map((s, j) => (
                            <li key={j}>{s.title} — {s.fault_class} ({(s.similarity * 100).toFixed(0)}% similar)</li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                )}
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
