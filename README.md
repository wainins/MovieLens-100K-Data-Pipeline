# MovieLens 100K Data Pipeline using Apache Spark & Cassandra

<p align="center">
  <img width="1584" height="396" alt="Image" src="https://github.com/user-attachments/assets/072c5aa5-d032-4175-b942-96737dd97993" />
</p>

<p align="center">
  <a href="#overview">Overview</a> •
  <a href="#dataset">Dataset</a> •
  <a href="#analytical-tasks">Analytical Tasks</a> •
  <a href="#results">Results</a> •
  <a href="#cassandra-storage-and-validation">Cassandra</a> •
  <a href="#reproducibility">Reproducibility</a> •
  <a href="#author">Author</a>
</p>

> **Academic Project — STQD6324 Data Management**

> A distributed data processing project using Apache Spark and Apache Cassandra to analyse the MovieLens 100K dataset. This project demonstrates a complete data engineering workflow from data ingestion into HDFS, analytical querying with Spark SQL and storage in Cassandra.

---

# Overview

The objective is to build a Python-based data pipeline using Apache Spark and Cassandra to perform analytical queries on the MovieLens 100K dataset.

The project demonstrates:

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
### Notebook Structure

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
│   ├── Task 1
│   ├── Task 2
│   ├── Task 3
│   ├── Task 4
│   └── Task 5
│
└── 8. Write results to Cassandra
└── 9. Read back from Cassandra for validation
└── 10. Session Completion
```
---

# Dataset

### Source: [MovieLens 100K Dataset](https://grouplens.org/datasets/movielens/)↗️

### Files Used

| File | Fields | Records |
|------|--------|---------|
| `u.data` | user id, movie id, rating, timestamp | 100,003 |
| `u.user` | user id, age, occupation, gender, zip code | 943 |
| `u.item` | movie id, movie title, release date, video release date, IMDb URL, 19 genres | 1,682 |

---

# Analytical Tasks

The following analytical tasks were implemented using Apache Spark and results were stored in Apache Cassandra:

| Task No. | Description | Cassandra Table |
|----------|-------------|-----------------|
| 1 | Average rating for each movie | avg_movie_ratings |
| 2 | Top 10 movies by highest average rating | top10_movies |
| 3 | Favourite genre of active users (≥ 50 ratings) | user_fav_genre |
| 4 | Users under 20 years old | users_under20 |
| 5 | Scientists aged between 30 and 40 | scientists_30_40 |

---

# Results

## Task 1: Average Rating for Each Movie

The average rating and total number of ratings were calculated for every movie in the dataset.

### Result Table
<img width="1820" height="539" alt="Image" src="https://github.com/user-attachments/assets/8dfd6e10-ade5-45e7-8749-0780cc7707c2" />

### Visualisation
<img width="1779" height="600" alt="Image" src="https://github.com/user-attachments/assets/48cfb0cb-49e8-4861-914c-4299fe2576aa" />


### Findings

Movie ratings varied considerably across the dataset. Some movies achieved high average ratings despite receiving only a few reviews, highlighting the importance of considering rating counts alongside average scores.

---

## Task 2: Top 10 Movies with Highest Average Ratings

The ten highest-rated movies were identified based on average rating.

### Result Table
<img width="1844" height="541" alt="Image" src="https://github.com/user-attachments/assets/7e153477-8654-4684-9bff-b4d77591d153" />

### Visualisation
<img width="1642" height="556" alt="Image" src="https://github.com/user-attachments/assets/f637e36e-318e-46bb-8640-a8bd2d963dbd" />

### Findings

All top-ranked movies achieved a perfect average rating of 5.0. However, most of these movies received only one to three ratings, which does not necessarily indicate true popularity or quality. 

---

## Task 3: Favourite Genre of Users Who Rated at Least 50 Movies

Users with at least 50 ratings were identified and their favourite genre was determined based on the genre they rated most frequently.

### Result Table
<img width="1824" height="467" alt="Image" src="https://github.com/user-attachments/assets/7aaabeb2-635e-41db-b165-1df9691f5482" />

### Visualisation
<img width="1798" height="447" alt="Image" src="https://github.com/user-attachments/assets/855220cf-9b71-403c-a81f-701a9fc41eba" />

### Findings

Drama, Comedy, and Action were the most frequently rated genres among active users. Less common genres such as Documentary, Fantasy, and Film-Noir received significantly fewer ratings. This is likely due to due to lower engagement or fewer available titles in that genre.

---

## Task 4: Users Under 20 Years Old

Users younger than 20 years old were extracted from the dataset.

### Result Table
<img width="1824" height="465" alt="Image" src="https://github.com/user-attachments/assets/b51becd1-4ee3-49a5-b298-8e5e8dc2cb3a" />

### Visualisation
<img width="1768" height="500" alt="Image" src="https://github.com/user-attachments/assets/8c9c31a1-4b79-4b4f-83bb-bced1a7b7d42" />

### Findings


Most users under 20 were teenagers between 15 and 19 years old and were primarily classified as students (as expected).

---

## Task 5: Scientists Aged Between 30 and 40 Years Old

Users whose occupation was scientist and whose age was between 30 and 40 years old were identified.

### Result Table
<img width="1819" height="470" alt="Image" src="https://github.com/user-attachments/assets/4cf68e5f-027d-45e1-8923-8f60afe593ec" />>

### Visualisation
<img width="1770" height="429" alt="Image" src="https://github.com/user-attachments/assets/98a584cd-a9d6-4684-90f8-3c98654d6f64" />

### Findings

Only a small number of users matched the scientist occupation and age criteria. The results also showed a notable gender imbalance, with male users making up the majority of this group.

---

# Cassandra Storage and Validation

The analytical results generated by Apache Spark were written to Apache Cassandra for persistent storage.

## Cassandra Tables

| avg_movie_ratings | top10_movies | user_fav_genre | users_under20 | scientists_30_40 |
|---------|---------|---------|---------|---------|


### Cassandra Schema Example

```python
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

After writing the results to Cassandra, the tables were read back into Spark DataFrames to verify successful integration between Apache Spark and Apache Cassandra.

### Validation Example

```python
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

This project was developed and tested using the following environment:

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

1. Ensure HDP Sandbox is running.
2. Open Apache Zeppelin:
   http://localhost:9995
3. Apache Cassandra is running:
  ```bash
  pgrep -a java | grep cassandra
```
4. Spark interpreter in Zeppelin configured with:
```bash
spark.jars.packages = com.datastax.spark:spark-cassandra-connector_2.11:2.3.0
spark.cassandra.connection.host = 127.0.0.1
spark.cassandra.connection.port	= 9042
```
5. Import the notebook file:

```text
movielens_analysis_spark_cassandra.json
```

6. Execute the notebook paragraphs sequentially from top to bottom.

---

> [!NOTE]
> 1. All outputs presented in this repository were generated directly from the provided Zeppelin notebook.
> 2. Zeppelin was selected as the development environment because it provides native integration with Apache Spark which allow Spark jobs to be executed directly within interactive notebook paragraphs.
> 3. This notebook also supports built-in visualisation of query results which improves interpretability and reduces the need for external plotting tools.

---

<br>

## Author
* **Name:** Wan Ainin Sofiya binti Wan Mustafa
* **Matric No:** P160638



