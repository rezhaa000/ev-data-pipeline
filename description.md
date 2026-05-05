# Electric Vehicle Population Analysis - Washington State

## Repository Outline
```
1. description.md - Penjelasan gambaran umum project
2. EDA.ipynb - Notebook exploratory data analysis dan investigasi missing values
3. P2M3_Rezha_Aulia_DAG.py - DAG Airflow untuk otomatisasi pipeline data
4. P2M3_Rezha_Aulia_GX.ipynb - Notebook validasi data dengan Great Expectations
5. P2M3_Rezha_Aulia_data_raw.csv - Dataset original EV Population
6. P2M3_Rezha_Aulia_data_clean.csv - Dataset setelah proses data cleaning
7. P2M3_Rezha_Aulia_ddl.txt - DDL SQL dan DML untuk PostgreSQL
8. P2M3_Rezha_Aulia_conceptual.txt - Jawaban conceptual problems
9. images/  - Folder berisi screenshot dashboard Kibana beserta insight
```

## Problem Background

Industri kendaraan listrik (Electric Vehicle/EV) berkembang sangat pesat sebagai respons terhadap meningkatnya kesadaran lingkungan dan kebijakan pemerintah yang mendorong transisi energi bersih. Washington State menjadi salah satu negara bagian dengan tingkat adopsi EV tertinggi di Amerika Serikat.

Bagi perusahaan distributor EV, memahami pola adopsi, preferensi brand, dan distribusi geografis sangat krusial untuk menentukan strategi bisnis yang tepat. Sebagai Data Analyst di Divisi Marketing, report ini bertujuan untuk menganalisis tren adopsi EV di Washington State untuk membantu tim Sales dan Marketing dalam pengambilan keputusan bisnis.

## Project Output
Dashboard analitik interaktif menggunakan Kibana yang menampilkan:
- Distribusi tipe kendaraan listrik (BEV vs PHEV)
- Top 10 merek dan model EV terpopuler di Washington State
- Tren pertumbuhan registrasi EV per county di Washington State
- Distribusi geografis EV per county di Washington State
- Analisis kelayakan program insentif CAFV (Clean Alternative Fuel Vehicle)

## Data
- Sumber : Washington State Department of Licensing via Kaggle
- URL : https://www.kaggle.com/datasets/gunapro/electric-vehicle-population-data
- Jumlah baris : 130.443 (setelah cleaning: 130.138)
- Jumlah kolom : 17
- Rentang Model Year : 1997-2023
- Missing values : 
    - 3 baris non-Washington State (di-drop)
    - 222 missing di kolom `model` (diisi 'Unknown')
    - 305 missing di kolom `legislative_district` (diisi median)
    - 33 missing di kolom `vehicle_location` (diisi 'Unknown')
- Tipe data : Campuran kategorikal (8 kolom) dan numerikal (9 kolom)

## Method
1. Data Ingestion : Data raw di-load ke PostgreSQL menggunakan psycopg2
2. Data Pipeline : Automatisasi ETL menggunakan Apache Airflow dengan 3 task :
    - `fetch_from_postgresql` : ambil data dari PostgreSQL
    - `data_cleaning` : cleaning dan normalisasi data
    - `post_to_elasticsearch` : load data ke Elasticsearch
3. Data Validation : Validasi kualitas data menggunakan Great Expectations
4. Data Visualization : Dashboard EDA menggunakan Kibana dengan 6 visualisasi + 2 markdown

## Stacks
- Bahasa Pemograman : Python
- Database : PostgreSQL (via Docker)
- Pipeline : Apache Airflow
- Seach & Analytics Engine : Elasticsearch
- Data Visualization : Kibana
- Data Validation : Great Expectations
- Contenerization : Docker
- Libraries :
    - `pandas` - data manipulation
    - `psycopg2` - koneksi PostgreSQL
    - `elasticsearch` - koneksi ke Elasticsearch
    - `great_expectations` - validasi data
    - `matplotlib` dan `seaborn` - visualisasi EDA

## Reference
- [Dataset - Electric Vehicle Population Data (Kaggle)](https://www.kaggle.com/datasets/gunapro/electric-vehicle-population-data)
- [Sumber Data Asli - Washington State DOL](https://data.wa.gov/Transportation/Electric-Vehicle-Population-Data/f6w7-q2d2)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [IEA Global EV Outlook 2023](https://www.iea.org/reports/global-ev-outlook-2023)
- [Washington State ZEV Policy 2035](https://ecology.wa.gov/air-climate/reducing-greenhouse-gas-emissions/clean-vehicles)

---

**Referensi tambahan:**
- [Airflow, Elasticsearch, and Kibana](https://github.com/ardhiraka/DEBlitz)
- [Elasticsearch](https://www.elastic.co/docs)
- [Kibana](https://www.elastic.co/guide/en/kibana/index.html)
- [Docker](https://docs.docker.com/reference/compose-file/)