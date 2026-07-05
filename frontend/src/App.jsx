import { useState, useEffect } from 'react'
import { usePolling, simulatorStatus, infraStatus, kafkaStats, useIntervalFetch } from './api.js'
import StatusStrip from './components/StatusStrip.jsx'
import ChaosPanel from './components/ChaosPanel.jsx'
import PredictionsPanel from './components/PredictionsPanel.jsx'
import ServiceGrid from './components/ServiceGrid.jsx'
import IncidentFeed from './components/IncidentFeed.jsx'
import PropagationGraph from './components/PropagationGraph.jsx'
import DemoTimeline from './components/DemoTimeline.jsx'
import PipelineMap from './components/PipelineMap.jsx'
import GrafanaPanel from './components/GrafanaPanel.jsx'
import ArtifactViewer from './components/ArtifactViewer.jsx'
import ScoreboardStrip from './components/ScoreboardStrip.jsx'
import ModelCard from './components/ModelCard.jsx'

const TABS = ['Live Ops', 'Pipeline Map', 'Evidence']

export default function App() {
  const { state, error } = usePolling(2000)
  const [simStatus, setSimStatus] = useState(null)
  const [tab, setTab] = useState('Live Ops')

  const infra = useIntervalFetch(infraStatus, 5000, true)
  const kafkaActive = tab === 'Pipeline Map' || tab === 'Evidence'
  const kafka = useIntervalFetch(kafkaStats, 2000, kafkaActive)

  useEffect(() => {
    const id = setInterval(() => {
      simulatorStatus().then(setSimStatus).catch(() => {})
    }, 2000)
    simulatorStatus().then(setSimStatus).catch(() => {})
    return () => clearInterval(id)
  }, [])

  return (
    <div className="app">
      <header className="app__header">
        <h1>Aegis — Live SRE Console</h1>
        <nav className="app__tabs">
          {TABS.map((t) => (
            <button
              key={t}
              className={`app__tab ${tab === t ? 'app__tab--active' : ''}`}
              onClick={() => setTab(t)}
            >
              {t}
            </button>
          ))}
        </nav>
        {error && <span className="app__error">connection error: {error}</span>}
      </header>

      <StatusStrip infra={infra} totals={state?.totals} watermark={state?.watermark} />

      {tab === 'Live Ops' && state && (
        <>
          <ChaosPanel simStatus={simStatus} onChange={setSimStatus} />
          <PredictionsPanel predictions={state.predictions} watermark={state.watermark} />
          <ScoreboardStrip scoreboard={state.scoreboard} />
          <div className="app__row">
            <ServiceGrid services={state.services} />
            <div className="app__col">
              <PropagationGraph graph={state.graph} />
              <DemoTimeline state={state} simStatus={simStatus} />
            </div>
          </div>
          <IncidentFeed incidents={state.incidents} />
        </>
      )}

      {tab === 'Pipeline Map' && (
        <PipelineMap
          state={state}
          simStatus={simStatus}
          kafka={kafka}
          infra={infra}
          onSimChange={setSimStatus}
        />
      )}

      {tab === 'Evidence' && (
        <>
          <GrafanaPanel infra={infra} />
          <ModelCard />
          <ArtifactViewer />
        </>
      )}

      {!state && !error && <p className="app__loading">Connecting to Aegis…</p>}
    </div>
  )
}
