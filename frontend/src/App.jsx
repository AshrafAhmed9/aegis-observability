import { useEffect, useState } from 'react'

const FAULT_TYPES = ['redis_leak', 'queue_backlog', 'deadlock_burst']

export default function App() {
  const [incidents, setIncidents] = useState([])
  const [predictions, setPredictions] = useState([])
  const [mlInfo, setMlInfo] = useState(null)
  const [statusMessage, setStatusMessage] = useState('')

  useEffect(() => {
    refreshIncidents()
    refreshPredictions()
    refreshMlInfo()
    const interval = setInterval(() => {
      refreshIncidents()
      refreshPredictions()
      refreshMlInfo()
    }, 3000)
    return () => clearInterval(interval)
  }, [])

  async function refreshIncidents() {
    const response = await fetch('/incidents')
    setIncidents(await response.json())
  }

  async function refreshPredictions() {
    const response = await fetch('/predictions')
    setPredictions(await response.json())
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

      <div className="layout-grid">
        <div className="sidebar">
          <MlStatusCard mlInfo={mlInfo} />
          <PredictionsPanel predictions={predictions} />
        </div>
        <div className="main-column">
          <IncidentList incidents={incidents} />
        </div>
      </div>
    </div>
  )
}

function PredictionsPanel({ predictions }) {
  if (predictions.length === 0) return null
  return (
    <div className="card">
      <h3>ML Shadow Predictions</h3>
      <p className="subtitle">
        Scored alongside every event, before the deterministic engine decides root cause.
        Never overrides it -- shown here just to see what the model would have called.
      </p>
      <div className="prediction-list">
        {predictions.map((prediction, index) => (
          <div key={index} className="prediction-row">
            <span>{prediction.service}</span>
            <span className="badge">{prediction.severity}</span>
            <span className={`badge ${prediction.high_risk ? 'critical' : 'ok'}`}>
              risk {prediction.risk.toFixed(2)}
            </span>
          </div>
        ))}
      </div>
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
      {incidents.map((incident, index) => (
        // incident_id is derived only from the root-cause service name, so
        // it repeats across separate incidents (e.g. two INC-CACHE entries)
        // -- index is what's actually unique in this always-freshly-fetched list.
        <div className="card" key={index}>
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
