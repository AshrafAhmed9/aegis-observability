import { useEffect, useState } from 'react'

const FAULT_TYPES = ['redis_leak', 'queue_backlog', 'deadlock_burst']

export default function App() {
  const [incidents, setIncidents] = useState([])
  const [mlInfo, setMlInfo] = useState(null)
  const [statusMessage, setStatusMessage] = useState('')

  useEffect(() => {
    refreshIncidents()
    refreshMlInfo()
    const interval = setInterval(refreshIncidents, 3000)
    return () => clearInterval(interval)
  }, [])

  async function refreshIncidents() {
    const response = await fetch('/incidents')
    setIncidents(await response.json())
  }

  async function refreshMlInfo() {
    const response = await fetch('/ml/info')
    setMlInfo(await response.json())
  }

  async function injectFault(faultName) {
    setStatusMessage(`Injecting ${faultName}...`)
    const response = await fetch(`/simulate/${faultName}`, { method: 'POST' })
    const result = await response.json()
    setStatusMessage(`Sent ${result.event_count} events. Expected root cause: ${result.expected_root_cause}`)
  }

  return (
    <div className="app">
      <h1>Aegis</h1>
      <p className="subtitle">Streaming trace correlation + failure prediction</p>

      <div className="fault-buttons">
        {FAULT_TYPES.map((faultName) => (
          <button key={faultName} onClick={() => injectFault(faultName)}>
            Inject: {faultName.replace('_', ' ')}
          </button>
        ))}
      </div>

      {statusMessage && <p className="status-line">{statusMessage}</p>}

      <MlStatusCard mlInfo={mlInfo} />
      <IncidentList incidents={incidents} />
    </div>
  )
}

function MlStatusCard({ mlInfo }) {
  if (!mlInfo) return null
  return (
    <div className="card">
      <h3>
        ML Detector
        <span className={`badge ${mlInfo.available ? 'ok' : 'warn'}`}>
          {mlInfo.available ? 'available' : 'unavailable'}
        </span>
      </h3>
      {mlInfo.available && (
        <p>Drift level: {mlInfo.drift.level} (max PSI: {mlInfo.drift.max_psi.toFixed(3)})</p>
      )}
    </div>
  )
}

function IncidentList({ incidents }) {
  if (incidents.length === 0) {
    return <p className="empty">No incidents yet -- inject a fault above.</p>
  }
  return (
    <div>
      {incidents.map((incident) => (
        <div className="card" key={incident.incident_id}>
          <h3>
            {incident.incident_id}: {incident.title}
            <span className="badge critical">{incident.root_cause_class}</span>
          </h3>
          <p>{incident.hypothesis}</p>
        </div>
      ))}
    </div>
  )
}
