# Financial Market Data ETL Pipeline  
*A professional, scalable, and production-oriented ETL system for financial time series — currently under development.*

---

## 📌 Project Status  
🚧 **Work in Progress — Initial Development Stage**  
This repository outlines the vision, structure, and roadmap for a professional ETL (Extract, Transform, Load) pipeline designed for financial market data engineering.  
Implementation will be added progressively as the project evolves.

---

## 🎯 Project Overview  

Financial data is the foundation of quantitative research, algorithmic trading, and risk modeling.  
However, raw market data is often inconsistent, incomplete, and difficult to use directly.

This project aims to build a **fully automated and professional-grade ETL pipeline** capable of:

- Ingesting raw financial data from various formats (CSV, Parquet, API endpoints)  
- Cleaning, validating, and standardizing OHLC/Bid–Ask structures  
- Handling timezone normalization (UTC standard)  
- Detecting and correcting gaps, missing values, and duplicates  
- Resampling data across multiple timeframes  
- Exporting optimized datasets in Parquet format with modern compression (ZSTD)  
- Producing Data Quality Reports for transparency and auditing  

The final goal is to create a **high-performance data backbone** suitable for:

- Quantitative research  
- Market simulation & backtesting  
- Machine learning models  
- High-frequency and mid-frequency strategies  
- Academic research in financial time series  

---

## 🧱 Key Features (Planned)

### 🔹 **1. Extraction Layer**
- Batch ingestion of CSV and Parquet files  
- API connectors (planned)  
- Automatic schema detection  
- Validation of timestamps and OHLC structure  

### 🔹 **2. Transformation Layer**
- Standardization of column names  
- Normalization of Bid/Ask fields  
- Datetime processing and timezone alignment  
- Gap detection (session gaps, market holidays, missing candles)  
- Duplicate removal  
- Data repair strategies (ffill, bfill, interpolation)  
- Resampling to user-defined timeframes using industry-standard OHLC rules  

### 🔹 **3. Loading Layer**
- Export to Parquet  
- ZSTD compression for optimal storage & speed  
- Optional partitioning (e.g., by symbol, timeframe, year)  

### 🔹 **4. Data Quality Reports**
- Summary statistics  
- Missing-value diagnostics  
- Duplicate logs  
- Candle count verification  
- Outlier detection (planned)  
- Visual anomaly plots (planned)  

---

## 🏗️ Planned Architecture


```
financial-data-etl/
│
├── data/
│   ├── raw/               # Unprocessed input files
│   ├── processed/         # Cleaned and normalized outputs
│   └── reports/           # Data Quality Reports
│
├── src/
│   ├── extract/           # Ingestion logic
│   ├── transform/         # Normalization, cleaning, resampling
│   ├── load/              # Export utilities
│   └── utils/             # Helpers (logging, config, IO)
│
├── notebooks/             # Exploratory work & validation
├── tests/                 # Unit tests (future)
├── config/                # YAML configs for mappings & settings
└── README.md              # You are here
```

---

## 🚀 Goals of This Project

- Build a clean, modular, production-ready ETL system  
- Apply professional data engineering practices to financial datasets  
- Create a reusable architecture for future quant projects  
- Ensure transparency with reproducible reports and logged transformations  
- Provide a reliable data foundation for backtesting engines and ML models  

---

## 🧪 Technologies & Standards (Planned)

- **Python 3.11+**  
- **Pandas** for transformation  
- **PyArrow** & **Parquet** for storage  
- **Zstandard (ZSTD)** compression  
- **NumPy** for efficient operations  
- **Matplotlib** or **Plotly** for quality visuals  
- **PyTest** for unit testing  
- **YAML** configurations  
- **Pre-commit hooks** (formatting, linting)  

---

## 📅 Roadmap

### **Phase 1 — Project Initialization (Current)**
- Define scope and folder structure  
- Create base architecture  
- Write configuration templates  
- Establish coding standards  

### **Phase 2 — Extraction Layer**
- Bulk CSV/Parquet ingestion  
- Early validation and schema detection  
- Logging implementation  

### **Phase 3 — Transformation Layer**
- OHLC normalization  
- Timezone standardization  
- Gap/duplicate detection  
- Resampling engine  

### **Phase 4 — Loading Layer**
- Parquet export  
- ZSTD configuration options  
- Partitioning strategies  

### **Phase 5 — Data Quality Reporting**
- Summary reports  
- Diagnostics & plots  

### **Phase 6 — Optimization & Extensions**
- Vectorization improvements  
- API connectors  
- Multi-asset pipelines  
- Integration with backtesting engines  

---

## 🤝 Contributing  
This project is currently in its initial development phase.  
Contribution guidelines and issue templates will be added as the architecture becomes more stable.

---

## 📜 License  
To be defined as the project advances.

---

## 🙌 Acknowledgements  
This project is inspired by professional workflows found in quantitative funds, trading desks, and financial data engineering teams.

More updates and modules will be released progressively.

---
## Documentacion Inicial

1. `python -m venv .venv`
2. Activar: `source .venv/bin/activate`
3. `pip install -r requirements.txt` (o `poetry install`)
4. `pre-commit install`
5. `pre-commit run --all-files`
6. `make test` (opcional)
