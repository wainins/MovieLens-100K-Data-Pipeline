# MovieLens 100K Data Pipeline using Apache Spark & Cassandra

<p align="center">
  <img width="1584" height="396" alt="Image" src="https://github.com/user-attachments/assets/072c5aa5-d032-4175-b942-96737dd97993" />
</p>

<p align="center">
  <a href="#overview">Overview</a> •
  <a href="#dataset">Dataset</a> •
  <a href="#analysis">Analysis</a> •
  <a href="#results">Results</a> •
  <a href="#cassandra-storage-and-validation">Cassandra</a> •
  <a href="#reproducibility">Reproducibility</a>
</p>

> **Academic Project — STQD6324 Data Management**

> An academic big data analytics project using Apache Spark and Apache Cassandra to process and analyse the MovieLens 100K dataset. The project covers data ingestion into HDFS, data transformation, analytical querying with Spark SQL and storage in Cassandra.

---

# Overview

This project uses Apache Spark and Apache Cassandra to process and analyse the MovieLens 100K dataset. The workflow starts with loading the raw data into HDFS, followed by data transformation and cleaning using PySpark and Spark SQL. The analytical results are then stored in Cassandra and read back for validation.

The overall workflow is:

```mermaid
flowchart LR

A[MovieLens Dataset]
--> B[Upload to HDFS]

B --> C[Create RDDs]

C --> D[Convert to DataFrames]

D --> E[Data Cleaning]

E --> F[Spark SQL Analytics]

F --> G[Write Results to Cassandra]

G --> H[Read Back from Cassandra]

```
### Workflow Structure

```yaml
movielens_analysis_spark_cassandra.json
│
├── 1. Environment Configuration
├── 2. Download MovieLens dataset 
├── 3. Load Data into HDFS
├── 4. Parse Raw Data into RDDs
├── 5. Transform RDDs into Spark DataFrames
├── 6. Data cleaning & preprocessing
│
├── 7. Analytical queries using Spark SQL
│   ├── Average Movie Ratings
│   ├── Top 10 Movies by Average Rating
│   ├── Favourite Genres Among Active Users
│   ├── Users Under 20
│   └── Scientists Aged 30–40
│
└── 8. Write results to Cassandra
└── 9. Read back from Cassandra for validation
└── 10. Session Completion
```
---

# Dataset

**Dataset:** [MovieLens 100K](https://grouplens.org/datasets/movielens/)↗️

### Files Used

| File | Fields | Records |
|------|--------|---------|
| `u.data` | user id, movie id, rating, timestamp | 100,003 |
| `u.user` | user id, age, occupation, gender, zip code | 943 |
| `u.item` | movie id, movie title, release date, video release date, IMDb URL, 19 genres | 1,682 |

---

# Analysis

The following analyses were performed using Apache Spark, with the resulting outputs stored in Apache Cassandra:

| Analysis | Description | Cassandra Table |
|----------|-------------|-----------------|
| Average Movie Ratings | Average rating for each movie | `avg_movie_ratings` |
| Top 10 Movies | Movies with the highest average ratings | `top10_movies` |
| Favourite Genres | Favourite genre of users with at least 50 ratings | `user_fav_genre` |
| Users Under 20 | Users younger than 20 years old | `users_under20` |
| Scientists Aged 30–40 | Scientists between 30 and 40 years old | `scientists_30_40` |

---

# Results

## Average Movie Ratings

The average rating and number of ratings were calculated for each movie to examine overall rating patterns.

### Result Table
<img width="1820" height="539" alt="Image" src="https://github.com/user-attachments/assets/8dfd6e10-ade5-45e7-8749-0780cc7707c2" />

### Visualization
<img width="1779" height="600" alt="Image" src="https://github.com/user-attachments/assets/48cfb0cb-49e8-4861-914c-4299fe2576aa" />


### Findings

Average ratings varied across movies, with some movies receiving high average scores despite having only a small number of ratings. This shows why rating counts should be considered alongside average ratings when interpreting movie performance.

---

## Top 10 Movies by Average Rating

The top 10 movies were identified based on their average ratings.

### Result Table
<img width="1844" height="541" alt="Image" src="https://github.com/user-attachments/assets/7e153477-8654-4684-9bff-b4d77591d153" />

### Visualisation
<img width="1642" height="556" alt="Image" src="https://github.com/user-attachments/assets/f637e36e-318e-46bb-8640-a8bd2d963dbd" />

### Findings

All top-ranked movies achieved a perfect average rating of 5.0. However, most received only one to three ratings, so the high average scores should be interpreted cautiously and do not necessarily indicate greater popularity or overall quality.

---

## Favourite Genres Among Active Users

Users who rated at least 50 movies were analysed to identify the genres they rated most frequently.

### Result Table
<img width="1824" height="467" alt="Image" src="https://github.com/user-attachments/assets/7aaabeb2-635e-41db-b165-1df9691f5482" />

### Visualisation
<img width="1798" height="447" alt="Image" src="https://github.com/user-attachments/assets/855220cf-9b71-403c-a81f-701a9fc41eba" />

### Findings

Drama, Comedy and Action were the most frequently rated genres among active users, while Documentary, Fantasy and Film-Noir received fewer ratings. The results show a clear difference in rating frequency across genres within this group of active users.

---

## Users Under 20

Users younger than 20 years old were identified to examine the age distribution within this group.

### Result Table
<img width="1824" height="465" alt="Image" src="https://github.com/user-attachments/assets/b51becd1-4ee3-49a5-b298-8e5e8dc2cb3a" />

### Visualisation
<img width="1768" height="500" alt="Image" src="https://github.com/user-attachments/assets/8c9c31a1-4b79-4b4f-83bb-bced1a7b7d42" />

### Findings


Most users under 20 were between 15 and 19 years old, with students making up the largest occupational group in this age range.

---

## Scientists Aged 30–40

Users aged 30–40 whose occupation was classified as scientist were identified.

### Result Table
<img width="1819" height="470" alt="Image" src="https://github.com/user-attachments/assets/4cf68e5f-027d-45e1-8923-8f60afe593ec" />>

### Visualisation
<img width="1770" height="429" alt="Image" src="https://github.com/user-attachments/assets/98a584cd-a9d6-4684-90f8-3c98654d6f64" />

### Findings

Only a small number of users matched the scientist occupation and age criteria. Within this group, male users made up the majority, indicating a noticeable gender imbalance in the filtered results.

---

# Cassandra Storage and Validation

The analytical results generated by Apache Spark were stored in Apache Cassandra for persistent storage.

## Cassandra Tables

### Cassandra Tables

| Table | Purpose |
|---|---|
| `avg_movie_ratings` | Average rating and number of ratings for each movie |
| `top10_movies` | Top 10 movies by average rating |
| `user_fav_genre` | Favourite genre of users who rated at least 50 movies |
| `users_under20` | Users younger than 20 years old |
| `scientists_30_40` | Scientists aged 30–40 years old |


### Cassandra Schema Example

```sql
CREATE KEYSPACE IF NOT EXISTS movielens
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE movielens;
CREATE TABLE IF NOT EXISTS avg_movie_ratings (
    movie_id INT PRIMARY KEY,
    movie_title TEXT,
    avg_rating DOUBLE,
    num_ratings INT
);
...
```

---

## Validation

After storing the analytical results in Cassandra, the tables were read back into Spark DataFrames to validate the integration between Apache Spark and Apache Cassandra.

### Validation Example

```sql
spark.read \
.format("org.apache.spark.sql.cassandra") \
.options(table="top10_movies", keyspace="movielens") \
.load() \
.show()

```

### Validation Result

<img width="1265" height="463" alt="Image" src="https://github.com/user-attachments/assets/c3124c2b-f0f6-4f25-94c7-919ad68c86bc" />

---

# Reproducibility

## Development Environment

This project was developed and tested in the following environment:

| Component | Version |
|------------|------------|
| Apache Zeppelin | 0.7.3 |
| Apache Spark | 2.3.1 |
| PySpark | 2.3.1 |
| Apache Cassandra | 3.11.17 |
| CQLSH | 5.0.1 |
| Hadoop (HDFS) | HDP Sandbox |
| Python | 2.7.5 |

---

## Running the Project

The project was developed in an **HDP Sandbox environment using Apache Zeppelin, Apache Spark and Apache Cassandra**.

<details>
<summary>Environment Setup</summary>

1. Ensure HDP Sandbox is running.
2. Open Apache Zeppelin:
   `http://localhost:9995`
3. Ensure Apache Cassandra is running:

```bash
pgrep -a java | grep cassandra
```

4. Configure the Spark interpreter in Zeppelin:
```text
spark.jars.packages = com.datastax.spark:spark-cassandra-connector_2.11:2.3.0
spark.cassandra.connection.host = 127.0.0.1
spark.cassandra.connection.port = 9042
```
5. Import the notebook file:
```text
movielens_analysis_spark_cassandra.json
```

6. Execute the notebook paragraphs sequentially from top to bottom.
</details>

---

> [!NOTE]
> - All outputs in this repository were generated directly from the Zeppelin notebook.
> - Apache Zeppelin was used as the development environment for running Spark jobs and visualizing query results.

---

<br>

## About

This project was completed as part of the **STQD6324 Data Management** course at Universiti Kebangsaan Malaysia.



