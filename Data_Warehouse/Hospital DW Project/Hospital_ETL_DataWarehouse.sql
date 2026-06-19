
-- HOSPITAL MANAGEMENT DATA WAREHOUSE PROJECT

-- 1. CREATE DATABASE

CREATE DATABASE IF NOT EXISTS Hospital_Management;



-- 2. CREATE SOURCE TABLES (RAW OPERATIONAL DATA)

CREATE OR REPLACE TABLE patients (
    patient_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    city VARCHAR(50)
);

CREATE OR REPLACE TABLE doctors (
    doctor_id INT PRIMARY KEY,
    doctor_name VARCHAR(100),
    specialization VARCHAR(50),
    experience_years INT
);

CREATE OR REPLACE TABLE appointments (
    appointment_id INT PRIMARY KEY,
    patient_id INT,
    doctor_id INT,
    appointment_date VARCHAR(20),
    diagnosis VARCHAR(100),
    fee INT
);



-- 3. BRONZE LAYER 

CREATE OR REPLACE TABLE bronze_patients 
AS
SELECT * FROM patients;

CREATE OR REPLACE TABLE bronze_doctors 
AS
SELECT * FROM doctors;

CREATE OR REPLACE TABLE bronze_appointments 
AS
SELECT * FROM appointments;



-- 4. SILVER LAYER TRANSFORMATION - DOCTORS CLEANING

CREATE OR REPLACE VIEW vw_clean_doctors 
AS
SELECT DISTINCT
    doctor_id,
    COALESCE(TRIM(doctor_name), 'Unknown') AS doctor_name,
    COALESCE(TRIM(specialization), 'Unknown') AS specialization,
    COALESCE(experience_years, 0) AS experience_years,
    CASE
        WHEN COALESCE(experience_years,0) < 5 THEN 'Junior'
        WHEN COALESCE(experience_years,0) BETWEEN 5 AND 10 THEN 'Mid-Level'
        ELSE 'Senior'
    END AS experience_level
FROM bronze_doctors;

CREATE OR REPLACE TABLE silver_doctors 
AS
SELECT * FROM vw_clean_doctors;



-- 5. SILVER LAYER TRANSFORMATION - PATIENTS CLEANING

CREATE OR REPLACE VIEW vw_clean_patients
 AS
SELECT DISTINCT
    patient_id,
    first_name,
    last_name,
    CONCAT(first_name,' ',last_name) AS full_name,
    COALESCE(age,22) AS age,
    gender,
    COALESCE(NULLIF(city,''), 'Lahore') AS city,
    CASE
        WHEN COALESCE(age,22) < 18 THEN 'Minor'
        WHEN COALESCE(age,22) BETWEEN 18 AND 65 THEN 'Adult'
        ELSE 'Senior'
    END AS age_group
FROM bronze_patients;

CREATE OR REPLACE TABLE silver_patients
AS
SELECT * FROM vw_clean_patients;



-- 6. SILVER LAYER TRANSFORMATION - APPOINTMENTS CLEANING

CREATE OR REPLACE VIEW vw_clean_appointments 
AS
SELECT
    appointment_id,
    patient_id,
    doctor_id,
    TO_DATE(appointment_date,'YYYY-MM-DD') AS appointment_date,
    COALESCE(NULLIF(diagnosis,''), 'Not Diagnosed') AS diagnosis,
    COALESCE(fee,1500) AS fee,
    CASE
        WHEN COALESCE(fee,1500) < 1000 THEN 'Low'
        WHEN COALESCE(fee,1500) BETWEEN 1000 AND 2500 THEN 'Medium'
        ELSE 'High'
    END AS fee_category
FROM bronze_appointments;

CREATE OR REPLACE TABLE silver_appointments
AS
SELECT * FROM vw_clean_appointments;



-- 7. GOLD LAYER - DIMENSION TABLE: DOCTORS

CREATE OR REPLACE TABLE dim_doctors (
    doctor_key INT,
    doctor_id INT,
    doctor_name VARCHAR(100),
    specialization VARCHAR(50),
    experience_years INT,
    experience_level VARCHAR(20)
);

INSERT INTO dim_doctors
SELECT
    ROW_NUMBER() OVER(ORDER BY doctor_id) AS doctor_key,
    doctor_id,
    doctor_name,
    specialization,
    experience_years,
    experience_level
FROM silver_doctors;



-- 8. GOLD LAYER - DIMENSION TABLE: PATIENTS

CREATE OR REPLACE TABLE dim_patients (
    patient_key INT,
    patient_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(100),
    gender VARCHAR(10),
    city VARCHAR(50),
    age INT,
    age_group VARCHAR(20)
);

INSERT INTO dim_patients
SELECT
    ROW_NUMBER() OVER(ORDER BY patient_id) AS patient_key,
    patient_id,
    first_name,
    last_name,
    full_name,
    gender,
    city,
    age,
    age_group
FROM silver_patients;



-- 9. GOLD LAYER - DIMENSION TABLE: DATE

CREATE OR REPLACE TABLE dim_dates (
    date_key INT,
    full_date DATE,
    year INT,
    month_name STRING,
    day_name STRING
);

INSERT INTO dim_dates
SELECT
    ROW_NUMBER() OVER(ORDER BY full_date) AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    DATE_FORMAT(full_date,'MMMM') AS month_name,
    DATE_FORMAT(full_date,'EEEE') AS day_name
FROM (
    SELECT DISTINCT appointment_date AS full_date
    FROM silver_appointments
) x;



-- 10. GOLD LAYER - FACT TABLE

CREATE OR REPLACE TABLE fact_appointments (
    appointment_key INT,
    appointment_id INT,
    patient_key INT,
    doctor_key INT,
    date_key INT,
    diagnosis VARCHAR(100),
    fee_category VARCHAR(20),
    fee INT,
    appointment_count INT
);

INSERT INTO fact_appointments
SELECT
    ROW_NUMBER() OVER(ORDER BY sa.appointment_id) AS appointment_key,
    sa.appointment_id,
    dp.patient_key,
    dd.doctor_key,
    dt.date_key,
    sa.diagnosis,
    sa.fee_category,
    sa.fee,
    1 AS appointment_count
FROM silver_appointments sa
JOIN dim_patients dp
    ON sa.patient_id = dp.patient_id
JOIN dim_doctors dd
    ON sa.doctor_id = dd.doctor_id
JOIN dim_dates dt
    ON sa.appointment_date = dt.full_date;


-- 11. BUSINESS ANALYSIS QUERY - DOCTOR REVENUE RANKING

WITH doctor_revenue AS (
    SELECT
        d.doctor_name,
        d.specialization,
        SUM(f.fee) AS total_revenue
    FROM fact_appointments f
    JOIN dim_doctors d
        ON f.doctor_key = d.doctor_key
    GROUP BY d.doctor_name,d.specialization
)
SELECT *,
       RANK() OVER(ORDER BY total_revenue DESC) AS revenue_rank
FROM doctor_revenue;



-- 12. BUSINESS ANALYSIS QUERY - TOP 3 DIAGNOSES

WITH diagnosis_stats AS (
    SELECT
        diagnosis,
        COUNT(*) AS total_cases
    FROM fact_appointments
    GROUP BY diagnosis
)
SELECT *,
       DENSE_RANK() OVER(ORDER BY total_cases DESC) AS diagnosis_rank
FROM diagnosis_stats
QUALIFY diagnosis_rank <= 3;



-- 13. BUSINESS ANALYSIS QUERY - TOP SPENDING PATIENTS

WITH patient_spending AS (
    SELECT
        p.full_name,
        p.city,
        SUM(f.fee) AS total_spent
    FROM fact_appointments f
    JOIN dim_patients p
        ON f.patient_key = p.patient_key
    GROUP BY p.full_name,p.city
)
SELECT *,
       RANK() OVER(ORDER BY total_spent DESC) AS spending_rank
FROM patient_spending;



-- 14. BUSINESS ANALYSIS QUERY - MONTHLY REVENUE TREND

WITH monthly_revenue AS (
    SELECT
        d.month_name,
        d.year,
        SUM(f.fee) AS revenue
    FROM fact_appointments f
    JOIN dim_dates d
        ON f.date_key = d.date_key
    GROUP BY d.month_name,d.year
)
SELECT *,
       SUM(revenue) OVER(ORDER BY year) AS running_total_revenue
FROM monthly_revenue;



-- 15. BUSINESS ANALYSIS QUERY - REPEAT PATIENTS

WITH repeat_patients AS (
    SELECT
        p.full_name,
        COUNT(f.appointment_id) AS visits
    FROM fact_appointments f
    JOIN dim_patient p
        ON f.patient_key = p.patient_key
    GROUP BY p.full_name
)
SELECT *,
       ROW_NUMBER() OVER(ORDER BY visits DESC) AS visit_rank
FROM repeat_patients;



-- 16. BUSINESS ANALYSIS QUERY - DAILY REVENUE CHANGE

WITH daily_revenue AS (
    SELECT
        d.full_date,
        SUM(f.fee) AS total_revenue
    FROM fact_appointments f
    JOIN dim_date d
        ON f.date_key = d.date_key
    GROUP BY d.full_date
)
SELECT
    full_date,
    total_revenue,
    LAG(total_revenue) OVER(ORDER BY full_date) AS previous_day_revenue,
    total_revenue - LAG(total_revenue) OVER(ORDER BY full_date) AS revenue_difference
FROM daily_revenue;



-- 17. BUSINESS ANALYSIS QUERY - NEXT APPOINTMENT FEE

SELECT
    appointment_id,
    fee,
    LEAD(fee) OVER(ORDER BY appointment_id) AS next_appointment_fee
FROM fact_appointments;
