import { useEffect, useState } from 'react'
import { warRoomList, warRoomFile, evalScorecard } from '../api.js'

const SCORECARD = '__scorecard__'

export default function ArtifactViewer() {
  const [files, setFiles] = useState([])
  const [selected, setSelected] = useState(null)
  const [content, setContent] = useState(null)

  useEffect(() => {
    let cancelled = false
    async function refresh() {
      try {
        const data = await warRoomList()
        if (!cancelled) setFiles(data.files || [])
      } catch {
        if (!cancelled) setFiles([])
      }
    }
    refresh()
    const id = setInterval(refresh, 5000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  async function open(name) {
    setSelected(name)
    setContent(null)
    try {
      const data = name === SCORECARD ? await evalScorecard() : await warRoomFile(name)
      setContent(data.available ? data.content : '(not generated yet — run an incident first)')
    } catch (err) {
      setContent(`error loading artifact: ${err.message}`)
    }
  }

  return (
    <section className="panel artifact-viewer">
      <h2 className="panel__title">War Room Artifacts & Eval</h2>
      <div className="artifact-viewer__body">
        <ul className="artifact-viewer__list">
          {files.map((f) => (
            <li key={f.name}>
              <button
                className={`artifact-viewer__file ${selected === f.name ? 'artifact-viewer__file--active' : ''} ${!f.available ? 'artifact-viewer__file--missing' : ''}`}
                onClick={() => open(f.name)}
              >
                {f.name}
                {f.available && f.mtime && (
                  <span className="artifact-viewer__mtime">{new Date(f.mtime * 1000).toLocaleTimeString()}</span>
                )}
              </button>
            </li>
          ))}
          <li>
            <button
              className={`artifact-viewer__file ${selected === SCORECARD ? 'artifact-viewer__file--active' : ''}`}
              onClick={() => open(SCORECARD)}
            >
              eval scorecard
            </button>
          </li>
        </ul>
        <div className="artifact-viewer__content">
          {selected === null && <p className="artifact-viewer__empty">Select an artifact to view.</p>}
          {selected !== null && content === null && <p className="artifact-viewer__empty">Loading…</p>}
          {content !== null && <pre>{content}</pre>}
        </div>
      </div>
    </section>
  )
}
