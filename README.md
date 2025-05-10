# databricks_pyspark_overview
---

## Overview

This notebook provides a comprehensive analysis pipeline for exploring four key tables from the AdventureWorks database across two analytical layers:

### 1. **PySpark Layer (Databricks)**

- Initial data exploration of all four tables
- Basic transformations and structural analysis
- Core Spark concepts

### 2. **Power BI Desktop Layer**

- *Still in progress—check back later!*

## Current PySpark Implementation

The notebook currently focuses on:

1. **Loading and examining** four AdventureWorks tables:
    - `FactInternetSales`
    - `DimProducts`
    - `DimSalesTerritory`
    - `DimCurrency`
2. **Basic data preparation**
3. **Exploring PySpark concepts**, including:
    - Performance implications of **User-Defined Functions (UDFs)**
    - Different **join strategies** and their performance
    - few Key **Delta table** functionalities

## Pipeline Evolution

This is a **living document** that will:

- Gradually incorporate **new data sources**
- Expand with **advanced transformations**
- Integrate **additional Spark concepts** over time
- Serve as the **transformation pipeline** for Power BI dashboards

---

## Key Findings

### UDF vs. Native PySpark Performance

The following snippet compares two approaches for the same transformation (creating a calculated column)—one using a **UDF** and the other using **native PySpark syntax**.

Since this was tested on the `FactInternetSales` table (~60K rows), both executed quickly, but **native PySpark was noticeably faster** (first job) than the Pandas UDF approach.

![image.png](attachment:df81709d-5fbe-4fa1-8018-95cfe7da8d88:image.png)

- 
- **`WholeStageCodegen` time** reflects Spark’s execution of optimized JVM bytecode.
- **Pandas UDF overhead**:
    - Data must be **serialized** (JVM → Python via Arrow).
    - The UDF runs in Python, outside Spark’s JVM optimizations.
    - Results are **deserialized** back to the JVM.
    - This extra processing increases runtime
    
    ![image.png](attachment:25c7b25d-0535-4a18-8b0d-60b0aa42064a:image.png)
    
- **Native PySpark**:
    - but for the native code, the **`WholeStageCodegen`**  duration was 0 since The entire operation was so fast that Spark didn’t register measurable time (sub-millisecond execution).
    
    ![image.png](attachment:bfef68d7-d7b3-4add-83ee-b0a24a10723e:image.png)
    

---

## Join Strategies

We compared three join approaches (**Broadcast**, **Sort-Merge**, and **Bucketed Join**) by left-joining `FactInternetSales` (large) with `DimCurrency` (small). As expected, **Broadcast Join** performed best.

### 1. Broadcast Join

- Only the small `DimCurrency` table (3.1 KiB) was shuffled.
- The shuffle contained **10 records** (matching `DimCurrency`'s row count).

![image.png](attachment:8583f07d-f371-401f-bccf-26b9c0243d6d:image.png)

### 2. Sort-Merge Join

- Slower than Broadcast Join for this scenario (fact + dimension table join).

![image.png](attachment:5a33603f-e062-4eb0-9547-aa2e05829465:image.png)

### 3. Bucketed Join

- Generated **6 jobs** (vs. 2 for others) due to:
    - Bucket boundary optimization
    - Data sampling
    - Additional preparatory steps
    
    ![image.png](attachment:d6e8925f-5ab3-45d3-849c-1f707a346804:image.png)
    

---

## Delta Table Features

in this section we explore two delta table concepts: time travel, and Schema evolution

### Time Travel

This section demonstrates the **time travel** capabilities of Delta tables, which enable tracking the history of modifications. Each change creates a new version of the table, allowing you to **query both past and current versions** of your data. This ensures that unwanted changes do not corrupt your tables.

### Schema Evolution

Similar to **Time Travel**, **Schema Evolution** allows tables to adapt to changing requirements while maintaining backward compatibility. Just as **Time Travel** lets you revisit older data versions, schema history tracks and audits schema changes over time, such as added columns or modified data types.

Additionally, it enables you to **overwrite existing Delta tables** without encountering schema mismatch errors, even when columns have been renamed.
