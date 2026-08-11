
-- SILVER TABLE VALIDATION


-- View sample records
SELECT *
FROM silver_job_market
LIMIT 10;

-- Count total records
SELECT COUNT(*)
FROM silver_job_market;



-- GOLD LAYER: DIM_JOB


CREATE TABLE dim_job 
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY job_title, experience_level
    ) AS job_key,
    job_title,
    experience_level,
    employment_type,
    industry,
    education_level,
    primary_language,
    has_ml_in_title
FROM (
    SELECT DISTINCT
        job_title,
        experience_level,
        employment_type,
        industry,
        education_level,
        primary_language,
        has_ml_in_title
    FROM silver_job_market
) AS jobs;


-- Check dim_job
SELECT *
FROM dim_job
LIMIT 10;



-- GOLD LAYER: DIM_COMPANY


CREATE TABLE dim_company 
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY company_location, company_size
    ) AS company_key,
    company_size,
    company_location
FROM (
    SELECT DISTINCT
        company_size,
        company_location
    FROM silver_job_market
) AS companies;


-- Check dim_company
SELECT *
FROM dim_company
LIMIT 10;



-- GOLD LAYER: DIM_EMPLOYEE


CREATE TABLE dim_employee 
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY employee_residence, years_experience
    ) AS employee_key,
    employee_residence,
    years_experience,
    manages_people,
    team_size,
    certifications_count
FROM (
    SELECT DISTINCT
        employee_residence,
        years_experience,
        manages_people,
        team_size,
        certifications_count
    FROM silver_job_market
) AS employees;


-- Check dim_employee
SELECT *
FROM dim_employee
LIMIT 10;



-- GOLD LAYER: FACT_TABLE


CREATE TABLE fact_table 
AS
SELECT
    ROW_NUMBER() OVER () AS job_market_key,

    j.job_key,
    c.company_key,
    e.employee_key,
	s.remote_ratio,
    s.weekly_hours,
    s.uses_ai_tools_daily,
    s.ai_tools_hours_per_week,
    s.salary_currency,
    s.salary_usd,
    s.equity_offered_pct,
    s.bonus_pct,
    s.job_satisfaction_score,
    s.interviews_to_offer,
    s.switched_jobs_last_year,
    s.upskilling_hours_per_month,
    s.fears_ai_automation_score
FROM silver_job_market AS s
JOIN dim_job AS j
    ON s.job_title = j.job_title
    AND s.experience_level = j.experience_level
    AND s.employment_type = j.employment_type
    AND s.industry = j.industry
    AND s.education_level = j.education_level
    AND s.primary_language = j.primary_language
    AND s.has_ml_in_title = j.has_ml_in_title
JOIN dim_company AS c
    ON s.company_size = c.company_size
    AND s.company_location = c.company_location
JOIN dim_employee AS e
    ON s.employee_residence = e.employee_residence
    AND s.years_experience = e.years_experience
    AND s.manages_people = e.manages_people
    AND s.team_size = e.team_size
    AND s.certifications_count = e.certifications_count;


-- Check fact_table
SELECT *FROM fact_table;



-- GOLD LAYER: BUSINESS QUESTIONS




-- Q1. What is the average salary by job title?


WITH salary 
AS (
    SELECT
        j.job_title,
        ROUND(AVG(f.salary_usd)) AS avg_salary
    FROM fact_table AS f
    JOIN dim_job AS j
        ON f.job_key = j.job_key
    GROUP BY j.job_title
)
SELECT *
FROM salary
ORDER BY avg_salary DESC;



-- Q2. Which industry pays the most?


WITH industry_salary 
AS (
    SELECT
        j.industry,
        SUM(f.salary_usd) AS total_salary
    FROM fact_table AS f
    JOIN dim_job AS j
        ON f.job_key = j.job_key
    GROUP BY j.industry
)
SELECT *
FROM industry_salary
ORDER BY total_salary DESC;



-- Q3. What is the average salary by experience level?


WITH experience_salary 
AS (
    SELECT
        j.experience_level,
        ROUND(AVG(f.salary_usd)) AS avg_salary
    FROM fact_table AS f
    JOIN dim_job AS j
        ON f.job_key = j.job_key
    GROUP BY j.experience_level
)
SELECT *
FROM experience_salary
ORDER BY avg_salary DESC;


-- Q4. What is the average salary by education level?


WITH education_salary 
AS (
    SELECT
        j.education_level,
        ROUND(AVG(f.salary_usd)) AS avg_salary
    FROM fact_table AS f
    JOIN dim_job AS j
        ON f.job_key = j.job_key
    GROUP BY j.education_level
)
SELECT *
FROM education_salary
ORDER BY avg_salary DESC;



-- Q5. Do remote workers earn more?


WITH remote_salary 
AS (
    SELECT
        remote_ratio,
        SUM(salary_usd) AS total_salary
    FROM fact_table
    GROUP BY remote_ratio
)
SELECT *
FROM remote_salary
ORDER BY total_salary DESC;



-- Q6. Which company size pays the most?


WITH company_salary 
AS (
    SELECT
        c.company_size,
        SUM(f.salary_usd) AS total_salary
    FROM fact_table AS f
    JOIN dim_company AS c
        ON f.company_key = c.company_key
    GROUP BY c.company_size
)
SELECT *
FROM company_salary
ORDER BY total_salary DESC;


-- Q7. What is the average salary by country?


WITH country_salary 
AS (
    SELECT
        e.employee_residence,
        AVG(f.salary_usd) AS avg_salary
    FROM fact_table AS f
    JOIN dim_employee AS e
        ON f.employee_key = e.employee_key
    GROUP BY e.employee_residence
)
SELECT *
FROM country_salary
ORDER BY avg_salary DESC;



-- Q8. Do AI users earn more?


WITH ai_salary 
AS (
    SELECT
        uses_ai_tools_daily,
        AVG(salary_usd) AS avg_salary
    FROM fact_table
    GROUP BY uses_ai_tools_daily
)
SELECT *
FROM ai_salary
ORDER BY avg_salary DESC;



-- Q9. What is job satisfaction by experience level?


WITH satisfaction 
AS (
    SELECT
        j.experience_level,
        AVG(f.job_satisfaction_score) AS avg_satisfaction
    FROM fact_table AS f
    JOIN dim_job AS j
        ON f.job_key = j.job_key
    GROUP BY j.experience_level
)
SELECT *
FROM satisfaction
ORDER BY avg_satisfaction DESC;



-- Q10. Do more certifications lead to higher salaries?


WITH certification_salary 
AS (
    SELECT
        e.certifications_count,
        AVG(f.salary_usd) AS avg_salary
    FROM fact_table AS f
    JOIN dim_employee AS e
        ON f.employee_key = e.employee_key
    GROUP BY e.certifications_count
)
SELECT *
FROM certification_salary
ORDER BY certifications_count;