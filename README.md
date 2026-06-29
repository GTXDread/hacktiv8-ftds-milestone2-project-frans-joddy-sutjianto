# Judul Project
```
Milestone 3 - ETL Process with Airflow and Visualization using Kibana
```
## Repository Outline

```
1. README.md - Penjelasan gambaran umum project
2. P2M3_frans_joddy_sutjianto_DAG.py - File Python dengan list fungsi DAG yang akan dilakukan oleh Airflow
3. P2M3_frans_joddy_sutjianto_data_raw.csv - Dataset raw yang diambil dari Kaggle melalui PostgreSQL
4. P2M3_frans_joddy_sutjianto_data_clean.csv - Dataset yang sudah dibersihkan melalui fungsi DAG dalam Airflow
5. P2M3_frans_joddy_sutjianto_GX.ipynb - Jupyter Notebook yang berisikan Data Validasi menggunakan Great Expectations
```

## Problem Background
```
Dalam proses Supply Chain, produk yang gagal dalam proses inspeksi QC cenderung memiliki cost yang lebih tinggi dan jumlah revenue yang lebih rendah dibandingkan dengan produk yang lulus dalam proses inspeksi QC sehingga jika suatu perusahaan Supply Chain memiliki produk yang sering mengalami kegagalan dalam proses inspeksi QC akan mengurangi pendapatan yang dapat dihasilkan dan total cost yang semakin tinggi.
```

## Project Output
```
Visualisasi Kibana beserta Insight yang didapatkan berdasarkan visualisasi tersebut yang dihasilkan dari Pipeline data yang awalnya berasal dari PostgreSQL sampai masuk ke dalam Elasticsearch melalui proses cleaning terlebih dahulu.
```

## Data
```
Sumber Dataset: https://www.kaggle.com/datasets/amirmotefaker/supply-chain-dataset
Deskripsi Singkat: Dataset Supply Chain produk Startup Fashion dan Beauty
Jumlah Kolom: 24 kolom
Jumlah Baris: 100 baris
```

## Method
```
Method dalam milestone ini menggunakan proses scripting dalam Python dengan menggunakan Query SQL untuk mengambil data dari PostgreSQL, membersihkan data, dan proses masuknya data ke dalam Elasticsearch dimana data tersebut diolah menjadi visualisasi insight bisnis.
```

## Stacks
```
- Python
- SQL
- Docker
- Airflow
- Elasticsearch and Kibana
```

## Reference
Referensi Paper menjelaskan Problem dalam Dataset: https://drpress.org/ojs/index.php/ajst/article/download/29870/29295
