import Sparkline from './Sparkline.jsx'

function statusClass(status) {
  if (status === 'CRITICAL') return 'sev-critical'
  if (status === 'ERROR') return 'sev-serious'
  if (status === 'WARNING') return 'sev-warning'
  return 'sev-good'
}

const METRIC_LABELS = {
  latency_ms: 'Latency (ms)',
  connection_pool_usage: 'Pool usage',
  queue_depth: 'Queue depth',
  active_connections: 'Active conns',
}

export default function ServiceGrid({ services }) {
  return (
    <section className="panel">
      <h2 className="panel__title">Services</h2>
      <div className="service-grid">
        {services.map((svc) => {
          const metricEntries = Object.entries(svc.series).filter(([, s]) => s.length > 0)
          return (
            <div key={svc.name} className={`service-card ${statusClass(svc.status)}`}>
              <div className="service-card__head">
                <span className="service-card__status-dot" />
                <span className="service-card__name">{svc.name}</span>
                <span className="service-card__status-label">{svc.status}</span>
              </div>
              <div className="service-card__stats">
                <span>{svc.event_count} events</span>
                <span>{svc.error_count} errors</span>
              </div>
              {svc.last_error_class && (
                <div className="service-card__error-class">{svc.last_error_class}</div>
              )}
              {metricEntries.slice(0, 1).map(([metric, s]) => (
                <div key={metric} className="service-card__metric">
                  <span className="service-card__metric-label">{METRIC_LABELS[metric] || metric}</span>
                  <Sparkline series={s} color="var(--series-1)" />
                </div>
              ))}
            </div>
          )
        })}
      </div>
    </section>
  )
}
