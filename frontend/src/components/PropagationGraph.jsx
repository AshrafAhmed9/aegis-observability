const ROLE_ORDER = ['ROOT_CAUSE', 'CYCLE_MEMBER', 'SYMPTOM']

function roleClass(role) {
  if (role === 'ROOT_CAUSE') return 'sev-critical'
  if (role === 'CYCLE_MEMBER') return 'sev-serious'
  if (role === 'SYMPTOM') return 'sev-warning'
  return 'sev-good'
}

export default function PropagationGraph({ graph }) {
  const nodes = graph?.nodes || []
  const roles = graph?.roles || {}

  if (nodes.length === 0) {
    return (
      <section className="panel">
        <h2 className="panel__title">Propagation Graph</h2>
        <p className="propagation-graph__empty">No incident graph yet.</p>
      </section>
    )
  }

  const columns = ROLE_ORDER.map((role) => ({
    role,
    services: nodes.filter((n) => (roles[n.service_name] || 'SYMPTOM') === role),
  })).filter((c) => c.services.length > 0)

  const unassigned = nodes.filter((n) => !roles[n.service_name])
  if (unassigned.length > 0) {
    columns.push({ role: 'OTHER', services: unassigned })
  }

  return (
    <section className="panel">
      <h2 className="panel__title">Propagation Graph</h2>
      <div className="propagation-graph">
        {columns.map((col) => (
          <div key={col.role} className="propagation-graph__column">
            <span className="propagation-graph__column-label">{col.role.replace('_', ' ')}</span>
            {col.services.map((n) => (
              <div key={n.service_name} className={`propagation-node ${roleClass(col.role)}`}>
                {n.service_name}
              </div>
            ))}
          </div>
        ))}
      </div>
    </section>
  )
}
