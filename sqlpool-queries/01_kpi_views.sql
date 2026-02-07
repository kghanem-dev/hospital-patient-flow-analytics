/* ============================================================
   01_kpi_views.sql
   Synapse SQL Pool: KPI Views for Power BI
   ============================================================ */

-- Bed occupancy % by gender
CREATE VIEW dbo.vw_bed_occupancy AS
SELECT
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.bed_id END) * 1.0
        / NULLIF(COUNT(f.bed_id), 0) * 100 AS bed_occupancy_percent
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;
GO

-- Bed turnover rate by gender (events per unique bed)
CREATE VIEW dbo.vw_bed_turnover_rate AS
SELECT
    p.gender,
    COUNT(DISTINCT f.fact_id) * 1.0
        / NULLIF(COUNT(DISTINCT f.bed_id), 0) AS bed_turnover_rate
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;
GO

-- Total currently admitted patients by gender
CREATE VIEW dbo.vw_patient_demographics AS
SELECT
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.fact_id END) AS total_patients
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;
GO

-- Avg treatment duration (LOS) by department + gender
CREATE VIEW dbo.vw_avg_treatment_duration AS
SELECT
    d.department,
    p.gender,
    AVG(f.length_of_stay_hours) AS avg_treatment_duration
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;
GO

-- Patient volume trend (admissions over time) by gender
CREATE VIEW dbo.vw_patient_volume_trend AS
SELECT
    f.admission_date,
    p.gender,
    COUNT(DISTINCT f.fact_id) AS patient_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY f.admission_date, p.gender;
GO

-- Department inflow (currently admitted count) by department + gender
CREATE VIEW dbo.vw_department_inflow AS
SELECT
    d.department,
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.fact_id END) AS patient_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;
GO

-- Overstay patients (LOS > 50 hours) by department + gender
CREATE VIEW dbo.vw_overstay_patients AS
SELECT
    d.department,
    p.gender,
    COUNT(f.fact_id) AS overstay_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
WHERE f.length_of_stay_hours > 50
GROUP BY d.department, p.gender;
GO

-- Quick checks
SELECT * FROM dbo.vw_bed_occupancy;
SELECT * FROM dbo.vw_bed_turnover_rate;
SELECT * FROM dbo.vw_patient_demographics;
SELECT TOP 10 * FROM dbo.vw_patient_volume_trend ORDER BY admission_date DESC;
GO
