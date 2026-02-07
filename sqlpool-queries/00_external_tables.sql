/* ============================================================
   00_external_tables.sql
   Synapse SQL Pool: External Data Source + External Tables
   ============================================================

   Prereqs (Azure RBAC):
   - Grant Synapse Workspace Managed Identity access to ADLS Gen2:
     Storage Blob Data Contributor (at least on the container or account)

   Notes:
   - Replace placeholders (PASSWORD).
   - LOCATION paths assume Parquet-only exports under:
       patient_flow_gold/synapse_exports/<table_name>/

   ============================================================ */

-- 1) Master key (run once per database)
-- Choose your own strong password (store securely)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name LIKE '%DatabaseMasterKey%')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'REPLACE_WITH_STRONG_PASSWORD';
END;
GO

-- 2) Scoped credential using Managed Identity
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'storage_credential')
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL storage_credential
    WITH IDENTITY = 'Managed Identity';
END;
GO

-- 3) External data source (ADLS Gen2)
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'gold_data_source')
BEGIN
    CREATE EXTERNAL DATA SOURCE gold_data_source
    WITH (
        TYPE = HADOOP,
        LOCATION = 'abfss://gold@hospitalstorage10.dfs.core.windows.net/',
        CREDENTIAL = storage_credential
    );
END;
GO

-- 4) External file format (Parquet)
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFileFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT ParquetFileFormat
    WITH (FORMAT_TYPE = PARQUET);
END;
GO

/* ------------------------------------------------------------
   (Optional) Drop/recreate for clean reruns
   ------------------------------------------------------------ */
IF OBJECT_ID('dbo.vw_overstay_patients','V') IS NOT NULL DROP VIEW dbo.vw_overstay_patients;
IF OBJECT_ID('dbo.vw_department_inflow','V') IS NOT NULL DROP VIEW dbo.vw_department_inflow;
IF OBJECT_ID('dbo.vw_patient_volume_trend','V') IS NOT NULL DROP VIEW dbo.vw_patient_volume_trend;
IF OBJECT_ID('dbo.vw_avg_treatment_duration','V') IS NOT NULL DROP VIEW dbo.vw_avg_treatment_duration;
IF OBJECT_ID('dbo.vw_patient_demographics','V') IS NOT NULL DROP VIEW dbo.vw_patient_demographics;
IF OBJECT_ID('dbo.vw_bed_turnover_rate','V') IS NOT NULL DROP VIEW dbo.vw_bed_turnover_rate;
IF OBJECT_ID('dbo.vw_bed_occupancy','V') IS NOT NULL DROP VIEW dbo.vw_bed_occupancy;

IF OBJECT_ID('dbo.fact_patient_flow','U') IS NOT NULL DROP EXTERNAL TABLE dbo.fact_patient_flow;
IF OBJECT_ID('dbo.dim_department','U') IS NOT NULL DROP EXTERNAL TABLE dbo.dim_department;
IF OBJECT_ID('dbo.dim_patient','U') IS NOT NULL DROP EXTERNAL TABLE dbo.dim_patient;
GO

/* ------------------------------------------------------------
   5) External tables (point to Parquet-only export folders)
   ------------------------------------------------------------ */

-- DIM PATIENT (includes SCD2 fields)
CREATE EXTERNAL TABLE dbo.dim_patient (
    patient_id         VARCHAR(100),
    gender             VARCHAR(50),
    age                INT,
    effective_from     DATETIME2,
    surrogate_key      BIGINT,
    effective_to       DATETIME2,
    is_current         INT
)
WITH (
    LOCATION    = 'patient_flow_gold/synapse_exports/dim_patient/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);
GO

-- DIM DEPARTMENT
CREATE EXTERNAL TABLE dbo.dim_department (
    department     VARCHAR(200),
    hospital_id    INT,
    surrogate_key  BIGINT
)
WITH (
    LOCATION    = 'patient_flow_gold/synapse_exports/dim_department/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);
GO

-- FACT PATIENT FLOW
CREATE EXTERNAL TABLE dbo.fact_patient_flow (
    fact_id               BIGINT,
    patient_sk            BIGINT,
    department_sk         BIGINT,
    admission_time        DATETIME2,
    discharge_time        DATETIME2,
    admission_date        DATE,
    length_of_stay_hours  FLOAT,
    is_currently_admitted INT,
    bed_id                INT,
    event_ingestion_time  DATETIME2
)
WITH (
    LOCATION    = 'patient_flow_gold/synapse_exports/fact_patient_flow/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);
GO

-- Sanity checks
SELECT COUNT(*) AS fact_rows FROM dbo.fact_patient_flow;
SELECT COUNT(*) AS dim_patient_rows FROM dbo.dim_patient;
SELECT COUNT(*) AS dim_dept_rows FROM dbo.dim_department;
GO
