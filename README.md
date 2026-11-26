# Big Data — Benchmarking Distributed Processing Frameworks on Google Cloud

> **Benchmarking Distributed Data Processing Frameworks on Cloud Infrastructure: Scalability, Performance, and Quality Insights from Steam Reviews**

A comparative study of **Polars · Dask · Modin · Apache Spark** and **Hadoop MapReduce** over 500,000 Steam user reviews, executed on Google Cloud Dataproc clusters with two distinct topologies.

**Authors:** Castro Gomez, Percy Valentín · Martínez Palomino, Jorge Armando · Molina Álvarez, Carla Viviana · Montesinos Damian, Leonardo Alberto · Namihas Millan, Yitzhak Abraham  
**Institution:** UTEC — Faculty of Computer Science | Big Data Course | September 2026

---

## ⚡ Key Results at a Glance

| Framework | Total time (10 queries) | Cluster scope |
|---|---:|---|
| **Polars** | **0.567 s** | Master node only (Rust, in-memory) |
| Modin | 21.45 s | Master node only (local Ray) |
| Dask | 203.18 s | Master node only (LocalCluster) |
| Apache Spark | 376.97 s | Full cluster (YARN) |

> **Spark was 665× slower than Polars** running the exact same logic on the exact same data.  
> **Doubling worker nodes (2 → 4) produced zero speedup** in any framework — causes verified empirically via Spark event logs.

---

## 📁 Repository Structure

```
notebooks/
├── polars_2_workers.ipynb      # Architecture A — Polars
├── polars_4_workers.ipynb      # Architecture B — Polars
├── dask_2_workers.ipynb        # Architecture A — Dask
├── dask_4_workers.ipynb        # Architecture B — Dask
├── modin_2_workers.ipynb       # Architecture A — Modin
├── modin_4_workers.ipynb       # Architecture B — Modin
├── spark_2_workers.ipynb       # Architecture A — Apache Spark
├── spark_4_workers.ipynb       # Architecture B — Apache Spark
└── mapreduce_dataproc.ipynb    # Hadoop MapReduce (wordmean, secondarysort, terasort)

informe/                        # LaTeX source of the technical report
research_paper.md               # Research paper (English, ~6 pages)
```

> The dataset (700 MB CSV) is **not versioned** here — it exceeds GitHub's 100 MB per-file limit and lives in the project's GCS bucket.

---

## 🏗️ Cloud Architecture

### Infrastructure

- **Storage:** Google Cloud Storage bucket `bigdata-2026-02`, prefix `proyecto01/`
- **Compute:** Google Cloud Dataproc (ephemeral clusters, fully decoupled from storage)
- **Stack:** Dataproc image `2.3-debian12` · Spark 3.5.3 · Hadoop 3.3.6

### Two Cluster Topologies

| Component | Architecture A | Architecture B |
|---|---|---|
| Master | 1 × `n2-highmem-4` (4 vCPU / 32 GB RAM) | 1 × `n2-highmem-4` (4 vCPU / 32 GB RAM) |
| Workers | 2 × `n2-standard-4` (4 vCPU / 16 GB each) | 4 × `n2-standard-4` (4 vCPU / 16 GB each) |
| Total worker vCPUs | 8 | 16 |
| Resource manager | YARN | YARN |

Architecture A and B ran on the **same dataset from the same GCS bucket** — the only variable changed was the number of workers.

---

## 🔟 The 10 Queries

Each query was implemented identically across all four frameworks and timed independently:

| # | Query | Operation type | Key result |
|---|---|---|---|
| 1 | Dataset exploration & validation | Validation | 500,000 × 24; nulls only in `game` and `steam_china_location` |
| 2 | Duplicate removal | Deduplication | **184,191 duplicates removed** → 315,809 unique rows |
| 3 | Null treatment | Data cleaning | 0 rows dropped; `review` is 100% populated |
| 4 | Feature creation (`review_length`) | Transformation | Character count column added on 315,809 records |
| 5 | Positive review filter | Filter + metric | 266,640 positive reviews; global rate **84.43%** |
| 6 | Review count per game | Groupby + aggregation | 22,992 games; Counter-Strike 2 leads (1,923 reviews) |
| 7 | Recommendation % per game | Multi-aggregation + metric | Top spots dominated by 1–4 review titles (noise) |
| 8 | Avg playtime per game | Mean aggregation | Max avg 2.18M minutes (outlier effect) |
| 9 | Avg review length per game | Mean aggregation | Ceiling at **8,000 chars** — Steam's hard limit |
| 10 | Final game ranking (≥100 reviews) | Groupby + filter + sort | 609 qualifying games; *Left 4 Dead 2* leads (821 reviews, 100% positive) |

### How to Run

All notebooks were executed in JupyterLab on the Dataproc master node — one framework per session for isolated resource usage. The first cell downloads the dataset:

```bash
gsutil cp gs://bigdata-2026-02/proyecto01/steam_reviews_500k.csv /tmp/steam_reviews_500k.csv
```

To run locally, replace that path with a local file path. Note that Spark will fall back to local mode (losing real YARN distribution), and timing comparisons won't be valid.

---

## 🗺️ Hadoop MapReduce

Three programs from `hadoop-mapreduce-examples.jar` were executed over HDFS on a dedicated Dataproc cluster (Hadoop 3.3.6):

| Program | Input | Volume | Mappers | Reducers | Result |
|---|---|---:|---:|---:|---|
| `wordmean` | *Don Quixote* (Project Gutenberg) | 2.1 MB | 1 | 1 | Mean word length: **4.4446 chars** |
| `secondarysort` | MovieLens 100K | 779 KB | 1 | 1 | 943 user groups, secondary-sorted by movie ID |
| `terasort` | Synthetic (via `teragen`) | 100 MB | 21 | 7 | 1,000,000 records globally sorted; `teravalidate` passed ✓ |

**Notable debugging finding:** `wordmean` required 3 runs. The first two completed with status `completed successfully` but produced **NaN** (empty input file) and **Infinity** (7 reducers split the 2 output keys across separate `part-r-*` files; the program only reads `part-r-00000`). Job counters—not status messages—were the decisive diagnostic tool.

---

## 📊 Dataset

**Source:** [100 Million+ Steam Reviews](https://www.kaggle.com/datasets/kieranpoc/steam-reviews) (Kaggle)

**Working sample:** 500,000 records · 24 columns · 700 MB  
**Sampling method:** Random with replacement (`replace=True`, seed=42) from a ~496K-row source file

**Important — 36.8% duplicates in raw data:**  
The sampling with replacement introduced 184,191 exact duplicate rows. This was detected by Query 2 and verified with the coupon-collector expected-distinct formula (predicted 316,060 distinct rows; observed 315,809 — 0.08% deviation). All reported metrics are computed on the **deduplicated 315,809-row set**.

**Cross-framework null-key divergence:**  
Queries 6–9 group by `game`. Pandas-based tools (`groupby` default `dropna=True`) silently drop the 22 null-game rows, reporting 22,991 groups. Polars and Spark retain nulls as a separate group, reporting 22,992. This one-unit difference has zero impact on rankings but illustrates a critical engineering lesson: **framework migration is not semantically neutral**.

---

## 📄 Research Paper

A 6-page research paper summarizing methodology, findings, and conclusions is included as [`research_paper.md`](./research_paper.md). It follows standard academic paper structure (Abstract, Introduction, Dataset, Architecture, Results, Conclusions, References) and is ready to convert to PDF via Pandoc or any Markdown renderer.

---

## 🔑 Conclusions

1. **Polars dominates at this scale** — 0.567 s total vs. 376.97 s for Spark. For ~668 MB, distribution overhead dwarfs compute cost.
2. **Horizontal scaling requires sufficient data granularity** — Spark's AQE collapsed 1,000 configured shuffle partitions to 2 real partitions; Dynamic Allocation used only 3 executors across 2 of 4 available nodes.
3. **Data integrity > speed** — `multiLine=True` was kept despite causing a single-partition read bottleneck, because disabling it corrupted 27 records with internal newlines.
4. **Distributed cleaning pipelines catch hidden defects** — the 36.8% duplicate rate would have silently inflated all metrics if not caught by Query 2.
5. **Architecture A (1 Master + 2 Workers) with Polars** is the optimal cost-performance combination for this data volume; Architecture B adds cost with no measurable benefit.
