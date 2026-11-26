# Benchmarking Distributed Data Processing Frameworks on Cloud Infrastructure: Scalability, Performance, and Quality Insights from Steam Reviews



## Abstract

This paper presents a comparative empirical evaluation of four distributed data processing frameworks—Polars, Dask, Modin, and Apache Spark—applied to 500,000 Steam user reviews on Google Cloud Dataproc. Two cluster topologies (1 Master + 2 Workers and 1 Master + 4 Workers) were evaluated across ten standardized queries covering data cleaning, transformation, filtering, grouping, aggregation, and ranking. Three Hadoop MapReduce programs (`wordmean`, `secondarysort`, `terasort`) were additionally executed over HDFS. Key findings include: (1) Polars completed all ten queries in **0.567 seconds**, making it **665× faster** than Apache Spark for this data volume; (2) doubling worker nodes produced no measurable speedup in any framework, explained by local execution modes (Polars, Dask, Modin) and Spark's Adaptive Query Execution collapsing parallelism; (3) distributed cleaning pipelines detected that **36.8% of input rows were exact duplicates**, an artifact of sampling with replacement; and (4) two semantically equivalent `groupby` implementations across frameworks silently diverged due to differing null-key defaults. The results demonstrate that horizontal scaling yields benefits only when data volume and task granularity generate sufficient parallelizable work units.

**Keywords:** Big Data, Distributed Processing, Apache Spark, Polars, Dask, Modin, Hadoop MapReduce, Google Cloud Dataproc, Scalability, Benchmarking.

---

## 1. Introduction

The exponential growth of digital interaction data has created a need for scalable processing architectures capable of transforming raw user-generated content into actionable business intelligence. Cloud-managed distributed computing platforms, such as Google Cloud Dataproc, offer on-demand cluster provisioning that decouples storage from computation—enabling cost-efficient, ephemeral workloads over large datasets.

This work addresses a practical and underexplored question: **how do modern data processing frameworks compare in performance, correctness, and scalability when applied to a medium-scale dataset on cloud infrastructure?** The digital video game market provides an ideal case study. Steam, the world's largest PC gaming distribution platform, generates millions of user reviews that capture player satisfaction, engagement, and behavioral signals across a catalog of tens of thousands of titles.

The dataset used is a 500,000-record sample from the *100 Million+ Steam Reviews* corpus (Kaggle), containing 24 variables per review including recommendation verdict, playtime metrics, community votes, and free-text content. All processing was performed over Google Cloud Storage and Google Cloud Dataproc clusters with Spark 3.5.3 and Hadoop 3.3.6.

The study makes the following contributions:

1. **Systematic performance benchmarking** of Polars, Dask, Modin, and Apache Spark across 10 equivalent queries and 2 cluster topologies, with timing exported from live cluster executions.
2. **Empirical scalability analysis**: quantifying why doubling worker nodes produced no speedup, with evidence from Spark event logs and executor allocation records.
3. **Data quality diagnosis**: detecting and verifying the probabilistic origin of 184,191 duplicate rows using the coupon-collector model.
4. **Cross-framework correctness verification**: identifying a systematic one-unit divergence in grouping results caused by differing null-key defaults across API families.
5. **Hadoop MapReduce execution analysis** across three programs, demonstrating how job counters serve as the primary debugging instrument in distributed environments.

A distinctive aspect of this report is that counterintuitive and negative results are documented with quantitative evidence rather than omitted—including two MapReduce jobs that completed with status `completed successfully` while producing invalid results (`NaN` and `Infinity`).

---

## 2. Dataset

### 2.1 Origin and Schema

The source dataset, *100 Million+ Steam Reviews*, was obtained from Kaggle and collected via the official Steam User Reviews API. In its full form, the dataset spans over 113 million historical records. The working file is a structured CSV with **500,000 records and 24 columns**, weighing **700.1 MB** (667.7 MiB) on disk.

Variables are organized into five semantic groups:

- **Identifiers:** `recommendationid`, `appid`, `game`
- **Player profile:** `author_steamid`, `author_num_games_owned`, `author_num_reviews`
- **Activity metrics (minutes):** `author_playtime_forever`, `author_playtime_last_two_weeks`, `author_playtime_at_review`, `author_last_played`
- **Review content and impact:** `language`, `review`, `timestamp_created`, `timestamp_updated`, `voted_up`, `votes_up`, `votes_funny`, `weighted_vote_score`, `comment_count`
- **Acquisition context:** `steam_purchase`, `received_for_free`, `written_during_early_access`, `hidden_in_steam_china`, `steam_china_location`

The schema assigns `Int64` to 19 numeric variables, `Float64` to `weighted_vote_score`, and `String` to the four text columns. Initial exploration revealed only two columns with null values: `game` (35 nulls, corresponding to titles delisted from the catalog) and `steam_china_location` (499,985 nulls, i.e., effectively empty).

### 2.2 Sampling and Duplicate Detection

The 500,000-record working file was constructed in Google Colab by sampling **with replacement** (`replace=True`, seed=42) from the first file returned by the Kaggle directory listing—not from the full 113-million-record corpus.

This decision introduced a measurable artifact. Direct measurement on the CSV revealed:

| Metric | Value |
|---|---|
| Total rows | 500,000 |
| Distinct `recommendationid` values | 315,031 |
| Distinct `(author_steamid, appid, review)` combinations | 315,809 |
| Distinct complete rows | 315,809 |
| **Exact duplicate rows** | **184,191 (36.8%)** |

The origin of duplicates was verified using the coupon-collector expected-distinct formula:

$$\mathbb{E}[\text{distinct}] = N\left[1 - \left(1 - \frac{1}{N}\right)^{n}\right]$$

With $n = 500{,}000$ and the 315,809 observed distinct rows, the implied source population size is $N \approx 496{,}000$. For $N = 500{,}000$, the formula predicts 316,060 distinct rows—a deviation of only **0.08%** from the measured 315,809. This confirms both that sampling was with replacement and that the source was a ~496,000-row file, not the full 113-million-record dataset.

All indicators reported in this paper are computed on the **deduplicated 315,809-row set**. The raw file was deliberately loaded into the Data Lake without preprocessing so that cleaning became part of the measured distributed workload.

---

## 3. Cloud Architecture on Google Cloud Platform

### 3.1 Data Lake on Google Cloud Storage

A Data Lake was implemented using a Google Cloud Storage bucket named `bigdata-2026-02`, provisioned in region `us-east4` (Northern Virginia) with Standard storage class. All project data was organized under the prefix `proyecto01/`, which stored both the input CSV and eight timing CSV files exported by notebooks after each execution.

The bucket was configured with no public internet access, isolating storage and preventing accidental data exposure. Cluster nodes accessed the CSV internally through the cluster's service account.

**Justification for cloud object storage over HDFS:** The Dataproc cluster is an ephemeral resource—created for testing and deleted afterward to avoid idle costs. If the dataset resided in HDFS within the cluster, it would be destroyed alongside it. By residing in GCS, the data survives any number of cluster cycles, and the same `gs://bigdata-2026-02/proyecto01/steam_reviews_500k.csv` object was consumable by all four frameworks from a single source of truth.

### 3.2 Cluster Topologies

Two Dataproc topologies were defined to study horizontal scalability:

| Parameter | Architecture A | Architecture B |
|---|---|---|
| Service | Cloud Dataproc | Cloud Dataproc |
| Image | `2.3-debian12` | `2.3-debian12` |
| Spark version | 3.5.3 | 3.5.3 |
| Master node | 1 × `n2-highmem-4` | 1 × `n2-highmem-4` |
| Master vCPU / RAM | 4 vCPU / 32 GB | 4 vCPU / 32 GB |
| Worker nodes | 2 × `n2-standard-4` | 4 × `n2-standard-4` |
| Worker vCPU / RAM (each) | 4 vCPU / 16 GB | 4 vCPU / 16 GB |
| Total worker vCPUs | 8 | 16 |
| Resource manager | YARN | YARN |
| Optional component | Jupyter | Jupyter |

The Master uses a high-memory instance because Polars, Dask, and Modin operate entirely on it: a 700 MB CSV with a high-cardinality free-text column expands several times in memory when materialized as a DataFrame, and a standard 16 GB instance would have risked Out-of-Memory failures.

The architecture follows a batch processing model with strict separation of storage and compute. Data flows from GCS → Master node → distributed processing (YARN for Spark/Hadoop) → results written back to GCS.

---

## 4. Distributed Processing and Performance Analysis

### 4.1 Execution Strategy and Measurement Protocol

Each framework was run in an independent notebook to give it exclusive access to 100% of Master resources. The measurement protocol was identical across all eight configurations:

1. The dataset is read once at notebook start; this initial read is **not** counted inside any query.
2. Each query is timed with `time.time()` immediately before and after the operation block.
3. For lazy frameworks (Dask, Spark), each query includes a materialization action (`.compute()`, `.count()`, or `.show()`) within the timed window to capture actual work executed, not mere task graph construction.
4. All ten timings are exported to `gs://bigdata-2026-02/proyecto01/tiempos_{framework}_{architecture}.csv`.

### 4.2 Framework Configurations

The four frameworks executed very differently within the same cluster environment:

| Framework | Execution mode | Cluster scope |
|---|---|---|
| Polars | Single-process, multi-threaded (Rust) | Master node only; reads from local disk after `gsutil` copy |
| Dask | Deferred scheduler over `LocalCluster` | Master node only; no YARN connection |
| Modin | Ray engine | Master node only; `ray.init()` without `address` creates a local cluster |
| Apache Spark | `SparkSession` over YARN | **Full cluster**: inherits `spark.master=yarn`, reads directly from `gs://` |

**Only Apache Spark executed work on worker nodes.** Polars, Dask, and Modin operated entirely on the Master, which explains a significant portion of the performance results.

### 4.3 Ten Queries: Coverage and Results

Ten queries were implemented equivalently across all four frameworks, covering the full range of required operation types:

| # | Operation | Type | Key Result |
|---|---|---|---|
| 1 | Dataset exploration | Validation | 500,000 rows × 24 cols; nulls only in `game` (35) and `steam_china_location` (499,985) |
| 2 | Duplicate removal | Deduplication | 184,191 duplicates removed; 315,809 unique rows remain |
| 3 | Null treatment | Data cleaning | 0 rows removed; `review` is 100% populated |
| 4 | Variable transformation | Feature engineering | `review_length` column created on 315,809 records |
| 5 | Positive review filtering | Filtering + metric | 266,640 positive reviews; global recommendation rate **84.43%** |
| 6 | Reviews per game | Groupby + aggregation | 22,992 games; Counter-Strike 2 leads with 1,923 reviews |
| 7 | Recommendation % per game | Multi-aggregation + metric | Top positions dominated by titles with 1–4 reviews (statistical noise) |
| 8 | Average playtime per game | Mean aggregation | Max average of 2.18M minutes; driven by outliers |
| 9 | Average review length per game | Mean aggregation | Ceiling at **8,000 characters** — Steam's hard review limit |
| 10 | Final game ranking | Groupby + filter + multi-sort | 609 games with ≥100 reviews; *Left 4 Dead 2* leads (821 reviews, 100% positive) |

### 4.4 Performance Results

**Architecture A (1 Master, 2 Workers):**

| Operation | Polars (s) | Modin (s) | Dask (s) | Spark (s) |
|---|---:|---:|---:|---:|
| Q1 — Exploration | 0.010 | 0.80 | 15.47 | 24.14 |
| Q2 — Duplicates | 0.321 | 8.82 | 19.33 | 27.44 |
| Q3 — Nulls | 0.004 | 2.17 | 49.19 | 81.98 |
| Q4 — Transform | 0.050 | 0.56 | 23.79 | 32.52 |
| Q5 — Filter | 0.008 | 1.84 | 35.38 | 46.28 |
| Q6 — Groupby | 0.046 | 0.99 | 11.72 | 34.42 |
| Q7 — Metrics % | 0.030 | 1.25 | 12.34 | 32.73 |
| Q8 — Averages | 0.033 | 3.21 | 11.79 | 32.32 |
| Q9 — Lengths | 0.034 | 0.70 | 12.14 | 32.31 |
| Q10 — Ranking | 0.031 | 1.12 | 12.04 | 32.84 |
| **Total** | **0.567** | **21.45** | **203.18** | **376.97** |

Polars completed all ten queries in **0.567 s**, while Spark required **376.97 s** — **665× slower**, executing identical logic on identical data and producing identical results. The Rust-native, columnar Arrow in-memory representation of Polars maximizes vertical memory usage of the Master node with no serialization overhead. Dask and Modin introduce orchestration overhead that penalizes datasets that fit comfortably in RAM.

**Scalability: Architecture A vs. Architecture B:**

| Framework | A (s) | B (s) | Δ (%) | Uses workers? |
|---|---:|---:|---:|---|
| Polars | 0.567 | 0.557 | −1.86 | No (local to Master) |
| Modin | 21.450 | 22.899 | +6.75 | No (local Ray) |
| Dask | 203.176 | 208.487 | +2.61 | No (LocalCluster) |
| Apache Spark | 376.972 | 368.819 | −2.16 | Yes (YARN) |

Variations between architectures range from −2.16% to +6.75%—within the normal measurement noise of a shared cloud environment. Notably, **Modin and Dask were slower on the larger cluster**, ruling out any marginal improvement interpretation.

### 4.5 Empirical Diagnosis: Why Spark Did Not Scale

Spark uniquely connected to the actual distributed cluster via YARN, yet showed no speedup. Three concurrent mechanisms were verified from the cluster's Spark event logs:

**a) Non-parallelizable read due to `multiLine=True`.** The dataset contains reviews with real line breaks inside quoted text fields. The `multiLine=True` option is required for data integrity, but forces Spark to treat the entire file as a single non-divisible unit:
```
Partitions after initial read (dfs.rdd.getNumPartitions()): 1
```
The 667.7 MiB parse runs on a single core regardless of available worker count.

**b) Adaptive Query Execution (AQE) collapses shuffle partitions.** Spark 3.5.3 on Dataproc 2.3-debian12 runs with `spark.sql.adaptive.enabled=true`. Although `spark.sql.shuffle.partitions=1000`, AQE dynamically merges partitions post-shuffle based on actual data volume. For 500,000 records, aggregations produce small intermediate results:
```
spark.sql.shuffle.partitions (configured): 1000
Actual partitions after groupBy("game").count(): 2
```

**c) Dynamic Allocation never requested additional resources.** With `spark.dynamicAllocation.enabled=true`, Spark requests new executors only when queued tasks demand them. Event log analysis for `application_1790005002581_0001` on Architecture B shows a maximum of **3 concurrent executors** across only 2 of the 4 worker nodes—6 cores active out of 16 available. The scheduler acted correctly: there was no work to distribute.

**Data integrity over parallelism.** Disabling `multiLine=True` would allow Spark to partition the read, but produces 500,027 records instead of 500,000—silently corrupting reviews with internal newlines. Data integrity was prioritized over read speed throughout all experiments.

---

## 5. Cross-Framework Correctness: Null Key Divergence

A systematic one-unit discrepancy was detected in Queries 6–9 (those grouping by game):

| Framework | Q6 | Q7 | Q8 | Q9 |
|---|---:|---:|---:|---:|
| Polars | 22,992 | 22,992 | 22,992 | 22,992 |
| Apache Spark | 22,992 | 22,992 | 22,992 | 22,992 |
| Dask | 22,991 | 22,991 | 22,992 | 22,992 |
| Modin | 22,991 | 22,991 | 22,991 | 22,991 |

**Root cause:** `groupby()` in pandas (and by inheritance in Modin and Dask) applies `dropna=True` by default, discarding rows where the grouping key is null. `group_by()` in Polars and `groupBy()` in Apache Spark **retain** null keys as a separate group. Query 3 had documented 22 null values in `game` (titles delisted from the catalog): $22{,}991 + 1 = 22{,}992$ accounts for the difference exactly.

The practical impact is negligible: 22 rows represent 0.007% of the clean dataset, and the minimum-volume threshold in Query 10 excludes them from the final ranking regardless. The engineering lesson is significant: **migrating a workload between frameworks is not a neutral operation**. Two syntactically equivalent implementations can silently diverge due to a single parameter's default value. The fix is to explicit the null-handling criterion (`dropna=False` in pandas-based tools) rather than relying on defaults.

---

## 6. Hadoop MapReduce on HDFS

Three programs from `hadoop-mapreduce-examples.jar` (Hadoop 3.3.6, Dataproc image `2.3-debian12`) were executed over a dedicated cluster (`n2-highmem-4` master, 2 × `n2-standard-4` workers) with HDFS as both input and output filesystem.

| Program | Input dataset | Volume | Mappers | Reducers |
|---|---|---:|---:|---:|
| `wordmean` | *Don Quixote* (Project Gutenberg) | 2.1 MB | 1 | 1 |
| `secondarysort` | MovieLens 100K (GroupLens) | 779 KB | 1 | 1 |
| `teragen` | Synthetic (generated) | 100 MB | 21 | 0 |
| `terasort` | Output of `teragen` | 100 MB | 21 | 7 |

### 6.1 `wordmean`: Mean Word Length Over *Don Quixote*

The program computes mean word length by emitting two key-value pairs per word (keys `count` and `length`) and dividing their sums in the Reduce phase. The final result was **4.4446 characters/word**, consistent with Spanish literary text dominated by short function words.

**Two failed attempts preceded the correct result**, both completing with status `completed successfully`:

- **Attempt 1 — `NaN`:** A malformed `sed` pattern generated an empty 0-byte input file. Job counters revealed `Map input records=0` and `Bytes Read=0`: the result of 0/0.
- **Attempt 2 — `Infinity`:** The cluster's default 7 reducers split the two output keys (`count`, `length`) across separate `part-r-*` files. The `WordMean` source code hard-codes reading only `part-r-00000`, finding one key and treating the other as 0 — a known limitation of the example program. Fix: `−D mapreduce.job.reduces=1`.

The job counters, not the status messages, were the decisive diagnostic tool in both cases.

### 6.2 `secondarysort`: Secondary Sort Pattern on MovieLens 100K

This program demonstrates how to sort values within reducer groups—a problem not solved by MapReduce's default key-only ordering—using a composite key (`IntPair`), a custom partitioner (`FirstPartitioner`), and a custom grouping comparator (`FirstGroupingComparator`).

Input was derived from MovieLens 100K (`u.data`) by extracting user-movie ID pairs with `awk`. The job produced **943 groups** (matching the 943 distinct users in the dataset), confirmed independently by counting separator lines in HDFS output. For user 196, the unordered input sequence `{242, 393, 381, 251, 655, 67, ...}` was reorganized to `{8, 13, 25, 66, 67, 70, ...}` — ascending by movie ID within the user group, performed entirely by the framework, not by user code.

### 6.3 `terasort`: Distributed Sort Benchmark at 100 MB

`terasort` is the industry-standard benchmark for distributed sorting in Hadoop clusters. It runs in three chained phases: `teragen` (data generation) → `terasort` (sort) → `teravalidate` (order verification).

`teragen` produced 1,000,000 records (100 MB) across **21 `part-m-*` files** using 21 mappers, with 0 reduce tasks (a pure map-only job). `terasort` then applied `TotalOrderPartitioner`—which sampled 100,000 keys to determine partition boundaries—to sort across **21 mappers and 7 reducers**, producing a globally ordered output across all `part-r-*` files. `teravalidate` confirmed correctness with a single checksum line and **zero ordering violations**.

The contrast with `wordmean` and `secondarysort` illustrates the core parallelism principle: files below the HDFS default block size (128 MB) generate a single input split and a single mapper. A three-node cluster does not accelerate a 2 MB file. This conclusion was reached independently from the Spark scalability analysis in Section 4.

---

## 7. Business Indicators

The distributed processing pipeline generated five actionable indicators over the clean 315,809-record dataset:

1. **Global recommendation rate: 84.43%** (266,640 positive reviews). This is the baseline against which any individual title must be compared: a game with 80% approval is *below average* in this sample.
2. **Sampling defect: 36.8% of raw records were duplicates.** Any dashboard built on the raw file would overestimate review volume by more than one-third without raising any error signal.
3. **Catalog concentration: 2.6%.** Only 609 of 22,992 games (2.6%) reach 100 reviews. Percentage-based metrics require volume thresholds to be interpretable—without them, single-review titles dominate rankings.
4. **Top-ranked titles (≥100 reviews):** *Left 4 Dead 2* (821 reviews, 100% positive) and *Subnautica* (807, 100%) lead the statistically supported ranking.
5. **Review length structural ceiling: 8,000 characters.** Detected empirically in Query 9; this hard limit imposed by Steam's platform constrains any future NLP analysis over the `review` field.

---

## 8. Conclusions

**1. Decoupled cloud architecture proved its value.** Separating storage (Cloud Storage) from compute (ephemeral Dataproc clusters) enabled destroying and reprovisioning two distinct topologies over the same dataset without any data loss or reload. This decoupling was the prerequisite that made the comparative experiment possible.

**2. For 500,000 records, the fastest framework is the one that distributes nothing.** Polars completed all ten queries in 0.567 s vs. 376.97 s for Spark—665× slower, same logic, same data, same results. Spark's overhead of serialization, task scheduling, distribution, and collection exceeds the computation cost itself at this data volume. The break-even point where distributed engines offset their overhead lies above the scale evaluated here.

**3. Doubling worker nodes produced no speedup.** Variations between Architecture A and B stayed within −2.16% to +6.75%, consistent with measurement noise. Causes were empirically verified: Polars, Dask, and Modin never reached the additional workers; Spark's AQE and Dynamic Allocation correctly determined that the workload did not justify additional executors.

**4. Data integrity was prioritized over speed.** Disabling `multiLine=True` would have enabled parallel reads but corrupted records with internal newlines. A faster result on corrupt data is not a valid result.

**5. Distributed cleaning pipelines reveal hidden data quality issues.** The deduplication phase detected and quantified a 36.8% duplicate rate traceable to a methodological decision in the sampling script. The probabilistic model verified the finding to within 0.08% deviation. This exemplifies the core purpose of a data quality validation stage in a Big Data pipeline.

**6. Framework migration is not semantically neutral.** A one-parameter default difference (`dropna=True` vs. null-group retention) silently produced different group counts across API families without any warning. In production environments with higher null rates, this discrepancy would be a real source of error.

**7. Hadoop MapReduce confirmed the parallelism threshold independently.** Small files (779 KB, 2.1 MB) generated one mapper each; 100 MB generated 21. HDFS block size determines achievable parallelism, and a three-node cluster does not accelerate a 2 MB job—the same conclusion reached through Spark's event logs, obtained through a completely independent path.

**Recommendation:** For the evaluated volume and topology, **Architecture A (1 Master + 2 Workers) with Polars** is the optimal cost-performance combination. Architecture B yields no economic justification. Genuine horizontal scaling benefits would require: (a) data volume an order of magnitude larger; (b) re-engineering input format to pre-partitioned Parquet (eliminating both the `multiLine` bottleneck and enabling parallel reads); or (c) explicit Spark configuration with static executor allocation and AQE disabled. The fundamental engineering lesson: **adding hardware does not accelerate a job the scheduler cannot or need not divide.**

---

## References

1. Kaggle. *100 Million+ Steam Reviews*. kieranpoc. Available at: https://www.kaggle.com/datasets/kieranpoc/steam-reviews/data
2. Google Cloud. *Cloud Dataproc Documentation*. https://cloud.google.com/dataproc/docs
3. Apache Software Foundation. *Apache Spark 3.5 Documentation*. https://spark.apache.org/docs/3.5.3/
4. Apache Software Foundation. *Apache Hadoop 3.3.6 Release*. https://hadoop.apache.org/docs/r3.3.6/
5. Polars Authors. *Polars User Guide*. https://docs.pola.rs/
6. Dask Development Team. *Dask Documentation*. https://docs.dask.org/
7. Modin Authors. *Modin Documentation*. https://modin.readthedocs.io/
8. Harper, F. M., & Konstan, J. A. (2015). The MovieLens Datasets: History and Context. *ACM Transactions on Interactive Intelligent Systems*, 5(4), 19:1–19:19.
9. Project Gutenberg. *Don Quijote de la Mancha* — eBook #2000. https://www.gutenberg.org/ebooks/2000
10. Zaharia, M., et al. (2016). Apache Spark: A Unified Engine for Big Data Processing. *Communications of the ACM*, 59(11), 56–65.
11. O'Malley, O. (2008). Terabyte Sort on Apache Hadoop. Yahoo!, Inc. Available at: http://sortbenchmark.org/YahooHadoop.pdf
