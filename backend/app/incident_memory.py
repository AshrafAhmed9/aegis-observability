"""Incident similarity search over a local corpus of past incidents.

Encoder priority: sentence-transformers (all-MiniLM-L6-v2) if installed,
else scikit-learn TF-IDF (always available once requirements-ml.txt is
installed, since scikit-learn is already required for the other models).
The corpus itself (ml/artifacts/incident_corpus.json) is plain text and
metadata — encoder-agnostic — so switching encoders never requires
rebuilding the corpus, only re-encoding it (done once at process start).

Guarded: with no ML deps, get_memory() returns None and callers skip
attaching similar_incidents — the rest of the pipeline is unaffected.
"""
import json
import os
from typing import List, Optional

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PATH = os.path.join(BACKEND_DIR, "ml", "artifacts", "incident_corpus.json")


class IncidentMemory:
    def __init__(self, incidents, vectors, encoder_name, encode_fn):
        self.incidents = incidents          # list of dict metadata
        self.vectors = vectors              # numpy array, one row per incident
        self.encoder_name = encoder_name
        self._encode_fn = encode_fn         # str -> 1D numpy vector (L2-normalized)

    @classmethod
    def load(cls) -> Optional["IncidentMemory"]:
        if not HAS_NUMPY or not os.path.exists(CORPUS_PATH):
            return None
        with open(CORPUS_PATH) as f:
            data = json.load(f)
        incidents = data.get("incidents", [])
        if not incidents:
            return None

        signatures = [inc["signature"] for inc in incidents]
        encoder_name, encode_fn, vectors = None, None, None

        try:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer("all-MiniLM-L6-v2")
            vectors = np.array(model.encode(signatures, normalize_embeddings=True))

            def encode_fn(text):
                return np.array(model.encode([text], normalize_embeddings=True))[0]
            encoder_name = "sentence-transformers/all-MiniLM-L6-v2"
        except ImportError:
            pass

        if vectors is None:
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                vectorizer = TfidfVectorizer(stop_words="english")
                raw = vectorizer.fit_transform(signatures).toarray()
                norms = np.linalg.norm(raw, axis=1, keepdims=True)
                norms[norms == 0] = 1.0
                vectors = raw / norms

                def encode_fn(text):
                    v = vectorizer.transform([text]).toarray()[0]
                    n = np.linalg.norm(v)
                    return v / n if n > 0 else v
                encoder_name = "tfidf"
            except ImportError:
                return None

        return cls(incidents, vectors, encoder_name, encode_fn)

    def add(self, incident: dict) -> None:
        """Append a newly processed incident so future queries can match
        against it too (in-memory only; the corpus file is not rewritten)."""
        vector = self._encode_fn(incident["signature"])
        self.incidents.append(incident)
        self.vectors = np.vstack([self.vectors, vector])

    def similar(self, signature: str, top_k: int = 3) -> List[dict]:
        query = self._encode_fn(signature)
        scores = self.vectors @ query  # both L2-normalized -> cosine similarity
        order = np.argsort(-scores)[:top_k]
        return [
            {
                "title": self.incidents[i]["title"],
                "fault_class": self.incidents[i]["fault_class"],
                "similarity": float(scores[i]),
            }
            for i in order
        ]


_memory: Optional[IncidentMemory] = None
_attempted = False


def get_memory() -> Optional[IncidentMemory]:
    """Lazily loads once per process; returns the same instance afterward
    so add() calls accumulate across incidents."""
    global _memory, _attempted
    if not _attempted:
        _attempted = True
        _memory = IncidentMemory.load()
    return _memory
