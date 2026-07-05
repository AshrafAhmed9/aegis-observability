import { useEffect, useRef, useState } from 'react'
import { mlInfo, mlRetrainStart, mlRetrainStatus, mlRollback } from '../api.js'

function CalibrationTable({ calibration }) {
  const [open, setOpen] = useState(false)
  if (!calibration) return null
  return (
    <div className="model-card__calibration">
      <button type="button" className="model-card__calibration-toggle" onClick={() => setOpen(!open)}>
        {open ? '▾' : '▸'} calibration: ECE {calibration.ece.toFixed(4)} · Brier {calibration.brier_score.toFixed(4)}
      </button>
      {open && (
        <table className="model-card__reliability">
          <thead>
            <tr><th>predicted bin</th><th>n</th><th>mean predicted</th><th>actual rate</th></tr>
          </thead>
          <tbody>
            {calibration.curve.map((c, i) => (
              <tr key={i}>
                <td>{c.bin_lo.toFixed(1)}–{c.bin_hi.toFixed(1)}</td>
                <td>{c.count}</td>
                <td>{(c.mean_predicted * 100).toFixed(1)}%</td>
                <td>{(c.actual_rate * 100).toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function FailureModelVersion({ v, onRollback, busy }) {
  const gbm = v.metrics?.gbm
  return (
    <li className={`model-card__version ${v.is_champion ? 'model-card__version--champion' : ''}`}>
      <div className="model-card__version-head">
        <span>v{v.version}{v.is_champion ? ' (champion)' : ''}</span>
        {!v.is_champion && (
          <button disabled={busy} onClick={() => onRollback(v.version)}>Rollback to this</button>
        )}
      </div>
      {gbm && (
        <div className="model-card__metrics">
          <span>PR-AUC {gbm.pr_auc.toFixed(4)}</span>
          <span>precision {(gbm.precision_at_threshold * 100).toFixed(0)}%</span>
          <span>recall {(gbm.recall_at_threshold * 100).toFixed(0)}%</span>
          <span>lead {gbm.median_lead_seconds != null ? `${gbm.median_lead_seconds.toFixed(0)}s` : '—'}</span>
        </div>
      )}
      <CalibrationTable calibration={gbm?.calibration} />
    </li>
  )
}

function RankerVersion({ v, onRollback, busy }) {
  const m = v.metrics
  return (
    <li className={`model-card__version ${v.is_champion ? 'model-card__version--champion' : ''}`}>
      <div className="model-card__version-head">
        <span>v{v.version}{v.is_champion ? ' (champion)' : ''}</span>
        {!v.is_champion && (
          <button disabled={busy} onClick={() => onRollback(v.version)}>Rollback to this</button>
        )}
      </div>
      {m && (
        <div className="model-card__metrics">
          <span>ML top-1 {(m.ml_top1_accuracy * 100).toFixed(0)}%</span>
          <span>Kahn top-1 {(m.kahn_top1_accuracy * 100).toFixed(0)}%</span>
          <span>agreement {(m.ml_kahn_agreement_rate * 100).toFixed(0)}%</span>
        </div>
      )}
    </li>
  )
}

export default function ModelCard() {
  const [info, setInfo] = useState(null)
  const [retrain, setRetrain] = useState(null)
  const [error, setError] = useState(null)
  const pollRef = useRef(null)

  async function refresh() {
    try {
      setInfo(await mlInfo())
    } catch {
      setInfo(null)
    }
  }

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 5000)
    return () => clearInterval(id)
  }, [])

  useEffect(() => () => clearInterval(pollRef.current), [])

  function pollRetrain() {
    pollRef.current = setInterval(async () => {
      const status = await mlRetrainStatus()
      setRetrain(status)
      if (status.state === 'done' || status.state === 'error') {
        clearInterval(pollRef.current)
        refresh()
      }
    }, 1500)
  }

  async function startRetrain() {
    setError(null)
    try {
      await mlRetrainStart()
      setRetrain({ state: 'running', log: [], result: null })
      pollRetrain()
    } catch (err) {
      setError(err.message)
    }
  }

  async function rollback(modelKey, version) {
    setError(null)
    try {
      await mlRollback(modelKey, version)
      refresh()
    } catch (err) {
      setError(err.message)
    }
  }

  if (!info) {
    return (
      <section className="panel model-card">
        <h2 className="panel__title">Model Card</h2>
        <p className="model-card__empty">
          ML layer unavailable — install requirements-ml.txt and run ml/generate_dataset.py + train scripts.
        </p>
      </section>
    )
  }

  const busy = retrain?.state === 'running'
  const drift = info.drift

  return (
    <section className="panel model-card">
      <div className="model-card__head">
        <h2 className="panel__title">Model Card</h2>
        <button disabled={busy} onClick={startRetrain}>
          {busy ? 'Retraining…' : 'Retrain'}
        </button>
      </div>
      {error && <div className="model-card__error">{error}</div>}

      <div className="model-card__section">
        <h3>Drift</h3>
        <span className={`model-card__drift-badge model-card__drift-badge--${(drift?.level || 'unknown').toLowerCase()}`}>
          {drift?.available ? `${drift.level} (max PSI ${drift.max_psi})` : 'not available'}
        </span>
      </div>

      <div className="model-card__section">
        <h3>Failure Model</h3>
        <ul className="model-card__versions">
          {info.failure_model.versions.map((v) => (
            <FailureModelVersion key={v.version} v={v} busy={busy}
              onRollback={(version) => rollback('failure_model', version)} />
          ))}
        </ul>
      </div>

      <div className="model-card__section">
        <h3>RCA Ranker</h3>
        <ul className="model-card__versions">
          {info.rca_ranker.versions.map((v) => (
            <RankerVersion key={v.version} v={v} busy={busy}
              onRollback={(version) => rollback('rca_ranker', version)} />
          ))}
        </ul>
      </div>

      {retrain && (
        <div className="model-card__section">
          <h3>Retrain Progress</h3>
          <div className={`model-card__retrain-state model-card__retrain-state--${retrain.state}`}>
            {retrain.state}
          </div>
          {retrain.result && (
            <div className="model-card__gate-result">
              {Object.entries(retrain.result.models || {}).map(([key, m]) => (
                <div key={key} className={`model-card__gate ${m.promoted ? 'model-card__gate--pass' : 'model-card__gate--fail'}`}>
                  <strong>{key}</strong>: v{m.challenger_version} {m.promoted ? 'PROMOTED ✓' : 'REJECTED ✗'}
                  {!m.promoted && m.reasons.length > 0 && (
                    <ul>{m.reasons.map((r, i) => <li key={i}>{r}</li>)}</ul>
                  )}
                </div>
              ))}
            </div>
          )}
          {retrain.log?.length > 0 && (
            <pre className="model-card__log">{retrain.log.slice(-15).join('\n')}</pre>
          )}
        </div>
      )}
    </section>
  )
}
