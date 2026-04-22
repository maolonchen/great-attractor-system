# Great Attractor System

A research-oriented platform Great-Attractor for gravitational anomaly analysis and large-scale structure mapping of the Great Attractor (GA) region, based on embedding-based spectral similarity computation.

## Overview

The Great Attractor is a gravitational anomaly located in the Laniakea Supercluster, approximately 150–250 Mpc from the Milky Way. This system provides tools for:

- **Spectral similarity matching** — Embedding-based vector search against a curated catalog of known celestial objects in the GA region.
- **Observation data classification** — Automatically classify celestial objects from survey datasets by matching spectral features against the standard catalog.
- **Gravitational grade assignment** — Assign gravitational influence grades to detected anomalies based on proximity and mass estimates.

## Architecture

```
Great Attractor System
├── Data Ingestion         → Standard catalog construction from curated JSONL datasets
├── Embedding Pipeline     → High-dimensional vector encoding (X-dim) via embed
├── Vector Search Engine   → Milvus Lite (inner product similarity) for nearest-neighbor retrieval
├── Web Interface          → FastAPI + vanilla JS single-page application
└── Result Export          → Classification results exported as Excel/CSV
```

## Quick Start

### Start Web Service

```bash
python main.py
```

### Specify Port

```bash
python main.py --port 8080
```

### HTTPS Mode

```bash
python main.py --ssl-keyfile key.pem --ssl-certfile cert.pem --port 8666 --host 0.0.0.0
```

## Dependencies

- Python >= 3.10
- FastAPI + Uvicorn (web framework)
- Milvus Lite (local vector database)
- aiohttp (async HTTP client)
- openpyxl / pandas (data I/O)
- tqdm (progress tracking)

Install dependencies:

```bash
uv sync
```

## Project Structure

```
├── main.py                    # FastAPI application entry point
├── app/
│   ├── core/
│   │   ├── config.py          # Configuration (embedding endpoint, DB path, etc.)
│   │   ├── vectoring.py       # Milvus vector client & embedding retrieval
│   │   └── utils.py           # Async concurrent utilities
│   └── processors/
│       ├── standard_data_processor.py   # Build standard catalog from JSONL
│       ├── user_data_processor.py       # Classify observation data via vector search
│       └── excel_processor.py           # Excel ↔ CSV conversion
├── scripts/                   # CLI utilities for pipeline operations
├── data/
│   ├── standards/             # Standard catalog files (JSONL)
│   ├── raw/                   # Uploaded observation datasets
│   └── processed/             # Classification results
├── static/
│   └── index.html             # Web frontend
└── logs/                      # Application logs
```

## Data Pipeline

1. **Catalog Construction** — Parse the standard celestial object catalog (`standard.jsonl`), encode each entry's spectral description into a 4096-dimensional vector, and store in a Milvus Lite collection.
2. **Observation Classification** — For each observation record, encode the spectral annotation, search the vector catalog for the nearest match, and append classification + grade + matched object metadata.
3. **Result Export** — Output classified datasets as downloadable Excel files.

## Configuration

Key settings are defined in `app/core/config.py`:

| Setting | Description |
|---|---|
| Embedding API | embed endpoint for spectral vector encoding |
| Milvus DB Path | Local Milvus Lite database file |
| Collection Name | `standard_telecom` (historical name, stores the celestial catalog) |
| Embedding Dimension | 4096 |

## License

Internal research use only.
