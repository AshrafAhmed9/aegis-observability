const DASH_PATH = '/d/aegis-main/aegis-observability?orgId=1&refresh=5s&kiosk&theme=dark'

export default function GrafanaPanel({ infra }) {
  const grafanaUp = Boolean(infra?.grafana)
  const base = infra?.urls?.grafana || 'http://localhost:3000'

  return (
    <section className="panel grafana-panel">
      <h2 className="panel__title">Grafana — provisioned dashboard</h2>
      {grafanaUp ? (
        <>
          <iframe
            className="grafana-panel__frame"
            src={`${base}${DASH_PATH}`}
            title="Aegis Grafana dashboard"
          />
          <a className="grafana-panel__link" href={`${base}${DASH_PATH}`} target="_blank" rel="noreferrer">
            Open full dashboard ↗
          </a>
        </>
      ) : (
        <div className="grafana-panel__fallback">
          <p>Grafana is offline. Bring up the observability stack:</p>
          <code>docker compose up kafka prometheus grafana</code>
          <p className="grafana-panel__fallback-note">
            The Aegis dashboard and Prometheus datasource are auto-provisioned — no manual setup.
          </p>
        </div>
      )}
    </section>
  )
}
