function severityClass(sev) {
  if (sev === 'CRITICAL') return 'sev-critical'
  if (sev === 'HIGH' || sev === 'ERROR') return 'sev-serious'
  if (sev === 'MEDIUM' || sev === 'WARNING') return 'sev-warning'
  return 'sev-good'
}

export default function IncidentFeed({ incidents }) {
  return (
    <section className="panel">
      <h2 className="panel__title">Incident Feed</h2>
      {incidents.length === 0 ? (
        <p className="incident-feed__empty">No incidents yet.</p>
      ) : (
        <ul className="incident-feed">
          {incidents.map((inc, i) => (
            <li key={i} className={`incident-row ${severityClass(inc.overall_severity)}`}>
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
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
