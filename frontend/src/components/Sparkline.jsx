import { useState, useMemo, useRef } from 'react'

const WIDTH = 160
const HEIGHT = 36
const PAD = 4

export default function Sparkline({ series, color = 'var(--series-1)', unit = '' }) {
  const svgRef = useRef(null)
  const [hover, setHover] = useState(null)

  const points = useMemo(() => {
    if (!series || series.length === 0) return []
    const values = series.map((p) => p[1])
    const min = Math.min(...values)
    const max = Math.max(...values)
    const range = max - min || 1
    const n = series.length
    return series.map(([ts, v], i) => {
      const x = n === 1 ? WIDTH / 2 : PAD + (i / (n - 1)) * (WIDTH - PAD * 2)
      const y = HEIGHT - PAD - ((v - min) / range) * (HEIGHT - PAD * 2)
      return { x, y, v, ts }
    })
  }, [series])

  if (points.length === 0) {
    return <div className="sparkline sparkline--empty">no data</div>
  }

  const path = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ')
  const last = points[points.length - 1]

  function handleMove(e) {
    const rect = svgRef.current.getBoundingClientRect()
    const relX = ((e.clientX - rect.left) / rect.width) * WIDTH
    let nearest = points[0]
    let nearestDist = Infinity
    for (const p of points) {
      const d = Math.abs(p.x - relX)
      if (d < nearestDist) {
        nearestDist = d
        nearest = p
      }
    }
    setHover(nearest)
  }

  return (
    <div className="sparkline">
      <svg
        ref={svgRef}
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        width="100%"
        height={HEIGHT}
        onMouseMove={handleMove}
        onMouseLeave={() => setHover(null)}
      >
        <path d={path} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
        <circle cx={last.x} cy={last.y} r="4" fill={color} stroke="var(--surface-1)" strokeWidth="2" />
        {hover && (
          <circle cx={hover.x} cy={hover.y} r="4" fill={color} stroke="var(--surface-1)" strokeWidth="2" />
        )}
      </svg>
      {hover && (
        <div className="sparkline__tooltip">
          {hover.v.toFixed(2)}{unit}
        </div>
      )}
    </div>
  )
}
