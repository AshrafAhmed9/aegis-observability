import { useState } from 'react'
import { usePrevious, kafkaReplay, simulatorStart, simulatorStop, simulatorInject, useIntervalFetch, mlInfo } from '../api.js'

const FAULTS = [
  { id: 'redis_connection_leak', label: 'Redis connection leak' },
  { id: 'queue_backlog', label: 'Queue backlog' },
  { id: 'deadlock_burst', label: 'Deadlock burst' },
]

const SCENARIOS = ['redis_retry_storm.log', 'pg_deadlock.log', 'cache_stampede.log']

function StageNode({ title, subtitle, status = 'ok', counters = [], children, hint }) {
  return (
    <div className={`stage-node stage-node--${status}`}>
      <div className="stage-node__title">{title}</div>
      {subtitle && <div className="stage-node__subtitle">{subtitle}</div>}
      {counters.length > 0 && (
        <div className="stage-node__counters">
          {counters.map((c) => (
            <div key={c.label} className="stage-node__counter">
              <span className="stage-node__counter-value">{c.value}</span>
              <span className="stage-node__counter-label">{c.label}</span>
            </div>
          ))}
        </div>
      )}
      {status === 'off' && hint && <div className="stage-node__hint">{hint}</div>}
      {children}
    </div>
  )
}

function Edge({ active }) {
  return (
    <div className={`stage-edge ${active ? 'stage-edge--active' : ''}`}>
      <span className="stage-edge__arrow">→</span>
    </div>
  )
}

function fmt(v) {
  if (v == null) return '—'
  return Number.isInteger(v) ? v : v.toFixed(1)
}

export default function PipelineMap({ state, simStatus, kafka, infra, onSimChange }) {
  const [scenario, setScenario] = useState(SCENARIOS[0])
  const [rate, setRate] = useState(5)
  const [replayError, setReplayError] = useState(null)
  const [busy, setBusy] = useState(false)

  const totals = state?.totals || {}
  const prevTotals = usePrevious(totals)
  const httpFlow = prevTotals && totals.events > prevTotals.events
  const httpIncidentFlow = prevTotals && totals.incidents > prevTotals.incidents

  const stats = kafka?.stats || {}
  const prevStats = usePrevious(stats)
  const consumerIngested = stats['aegis_events_ingested_total']
  const kafkaFlow = prevStats && consumerIngested > prevStats['aegis_events_ingested_total']
  const kafkaIncidents = stats['aegis_incidents_processed_total']
  const kafkaIncidentFlow = prevStats && kafkaIncidents > prevStats['aegis_incidents_processed_total']

  const replay = kafka?.replay || { state: 'idle' }
  const consumerOnline = Boolean(kafka?.consumer_online)
  const brokerUp = Boolean(infra?.kafka)

  const ml = useIntervalFetch(mlInfo, 5000, true)
  const mlAvailable = Boolean(ml?.ml_available)
  const mlPredictionCount = (state?.predictions ?? []).filter((p) => p.kind === 'ML_RISK').length

  async function runSim(fn) {
    setBusy(true)
    try {
      const result = await fn()
      onSimChange?.(result)
    } finally {
      setBusy(false)
    }
  }

  async function startReplay() {
    setBusy(true)
    setReplayError(null)
    try {
      await kafkaReplay(scenario, rate)
    } catch (err) {
      setReplayError(err.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="panel pipeline-map">
      <h2 className="panel__title">Pipeline Map — live architecture</h2>

      <div className="pipeline-lane">
        <div className="pipeline-lane__label">
          Live HTTP path
          <span className="pipeline-lane__sublabel">simulated fleet → in-process pipeline</span>
        </div>
        <div className="pipeline-lane__stages">
          <StageNode
            title="Simulated Fleet"
            subtitle="chaos source"
            status={simStatus?.running ? 'ok' : 'idle'}
            counters={simStatus?.fault ? [
              { label: 'fault', value: simStatus.fault.replace(/_/g, ' ') },
              { label: 'phase', value: simStatus.phase },
            ] : []}
          >
            <div className="stage-node__controls">
              {!simStatus?.running ? (
                <button disabled={busy} onClick={() => runSim(simulatorStart)}>Start</button>
              ) : (
                <button disabled={busy} onClick={() => runSim(simulatorStop)}>Stop</button>
              )}
              <select
                disabled={busy || !simStatus?.running}
                value=""
                onChange={(e) => e.target.value && runSim(() => simulatorInject(e.target.value))}
              >
                <option value="">Inject fault…</option>
                {FAULTS.map((f) => <option key={f.id} value={f.id}>{f.label}</option>)}
              </select>
            </div>
          </StageNode>
          <Edge active={httpFlow} />
          <StageNode
            title="Ingest"
            subtitle="POST /events"
            counters={[{ label: 'events', value: fmt(totals.events) }]}
          />
          <Edge active={httpFlow} />
          <StageNode
            title="Correlator + Predictor"
            subtitle="watermarks · EWMA/OLS"
            counters={[
              { label: 'traces', value: fmt(totals.traces) },
              { label: 'predictions', value: fmt(state?.predictions?.length ?? 0) },
            ]}
          />
          <Edge active={httpFlow} />
          <StageNode
            title="ML Layer"
            subtitle={mlAvailable ? `failure v${ml.failure_model.champion} · ranker v${ml.rca_ranker.champion}` : 'not installed'}
            status={mlAvailable ? 'ok' : 'off'}
            hint="pip install -r requirements-ml.txt && python ml/generate_dataset.py && python ml/train_failure_model.py"
            counters={mlAvailable ? [
              { label: 'ml risk', value: fmt(mlPredictionCount) },
              { label: 'drift', value: ml?.drift?.level ?? '—' },
            ] : []}
          />
          <Edge active={httpIncidentFlow} />
          <StageNode
            title="Topological RCA"
            subtitle="Kahn's algorithm"
            counters={[{ label: 'incidents', value: fmt(totals.incidents) }]}
          />
          <Edge active={httpIncidentFlow} />
          <StageNode
            title="War Room"
            subtitle="LLM + artifacts"
            counters={[{ label: 'exports', value: fmt(totals.incidents) }]}
          />
        </div>
      </div>

      <div className="pipeline-lane">
        <div className="pipeline-lane__label">
          Kafka path
          <span className="pipeline-lane__sublabel">horizontal-scale pipeline, observed via its Prometheus exposition</span>
        </div>
        <div className="pipeline-lane__stages">
          <StageNode
            title="Replay Producer"
            subtitle="keyed by trace_id"
            status={brokerUp ? 'ok' : 'off'}
            hint="docker compose up kafka"
            counters={replay.state !== 'idle' ? [
              { label: 'replay', value: replay.state },
              { label: 'sent', value: `${replay.sent}/${replay.total}` },
            ] : []}
          >
            <div className="stage-node__controls">
              <select disabled={busy} value={scenario} onChange={(e) => setScenario(e.target.value)}>
                {SCENARIOS.map((s) => <option key={s} value={s}>{s}</option>)}
              </select>
              <button disabled={busy || !brokerUp || replay.state === 'running'} onClick={startReplay}>
                Replay
              </button>
            </div>
            {replayError && <div className="stage-node__error">{replayError}</div>}
            {replay.error && <div className="stage-node__error">{replay.error}</div>}
          </StageNode>
          <Edge active={replay.state === 'running'} />
          <StageNode
            title="Kafka Broker"
            subtitle="topic: telemetry.raw"
            status={brokerUp ? 'ok' : 'off'}
            hint="docker compose up kafka"
          />
          <Edge active={kafkaFlow} />
          <StageNode
            title="Consumer Group"
            subtitle="aegis-correlators"
            status={consumerOnline ? 'ok' : 'off'}
            hint="cd backend && python3.12 -m app.consumer"
            counters={consumerOnline ? [
              { label: 'ingested', value: fmt(consumerIngested) },
              { label: 'traces', value: fmt(stats['aegis_traces_emitted_total']) },
              { label: 'open', value: fmt(stats['aegis_open_traces']) },
            ] : []}
          />
          <Edge active={kafkaIncidentFlow} />
          <StageNode
            title="RCA + Predictor"
            subtitle="STAT + ML detectors, same engine"
            status={consumerOnline ? 'ok' : 'off'}
            hint="starts with the consumer"
            counters={consumerOnline ? [
              { label: 'incidents', value: fmt(kafkaIncidents) },
              { label: 'predictions', value: fmt(stats['aegis_predictions_active']) },
              { label: 'late', value: fmt(stats['aegis_late_events_total']) },
            ] : []}
          />
          <Edge active={kafkaIncidentFlow} />
          <StageNode
            title="War Room"
            subtitle="shared artifacts"
            status={consumerOnline ? 'ok' : 'off'}
            hint="starts with the consumer"
          />
        </div>
      </div>

      <p className="pipeline-map__note">
        Both lanes run the identical StreamingCorrelator + FailurePredictor + RCA code. The Kafka lane
        is a separate OS process — this page reads it through the same Prometheus metrics endpoint
        (:9095) that Prometheus itself scrapes.
      </p>
    </section>
  )
}
