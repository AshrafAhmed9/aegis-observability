const SERVICES = [
  { key: 'api', label: 'API', hint: 'cd backend && python3.12 -m uvicorn app.main:app --port 8010' },
  { key: 'kafka', label: 'Kafka', hint: 'docker compose up kafka prometheus grafana' },
  { key: 'consumer', label: 'Consumer', hint: 'cd backend && python3.12 -m app.consumer' },
  { key: 'prometheus', label: 'Prometheus', hint: 'docker compose up kafka prometheus grafana' },
  { key: 'grafana', label: 'Grafana', hint: 'docker compose up kafka prometheus grafana' },
]

export default function StatusStrip({ infra, totals, watermark }) {
  const watermarkStr = watermark ? new Date(watermark * 1000).toLocaleTimeString() : '—'
  return (
    <div className="status-strip">
      <div className="status-strip__dots">
        {SERVICES.map((svc) => {
          const up = infra ? Boolean(infra[svc.key]) : false
          return (
            <span key={svc.key} className="status-strip__item" title={up ? `${svc.label} online` : `${svc.label} offline — ${svc.hint}`}>
              <span className={`status-strip__dot ${up ? 'status-strip__dot--up' : 'status-strip__dot--down'}`} />
              {svc.label}
            </span>
          )
        })}
      </div>
      {totals && (
        <div className="status-strip__totals">
          <span>{totals.events} events</span>
          <span>{totals.traces} traces</span>
          <span>{totals.incidents} incidents</span>
          <span>{totals.late} late</span>
          <span className="status-strip__clock">watermark {watermarkStr}</span>
        </div>
      )}
      <div className="status-strip__links">
        <a href="/docs" target="_blank" rel="noreferrer">API docs</a>
        {infra?.urls?.prometheus && <a href={infra.urls.prometheus} target="_blank" rel="noreferrer">Prometheus</a>}
        {infra?.urls?.grafana && <a href={infra.urls.grafana} target="_blank" rel="noreferrer">Grafana</a>}
      </div>
    </div>
  )
}
