SELECT * 
FROM 
"Gold_Table"

Select *
FROM
"Gold_Table"
LIMIT 10;

-- 1. Average Salary by Job Title

SELECT
    "Job Title",
    ROUND(AVG(
        NULLIF(
            REGEXP_REPLACE(
                SPLIT_PART("Salary Estimate", '-', 1),
                '[^0-9]', '', 'g'), ''
        )::NUMERIC
    )) AS average_salary
FROM "Gold_Table"
GROUP BY "Job Title"
ORDER BY average_salary DESC;



-- 2. Number of Jobs by Industry

SELECT
    "Industry",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Industry"
ORDER BY total_jobs DESC;



-- 3. Number of Jobs by Company Size

SELECT
    "Company_Size_Category",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Company_Size_Category"
ORDER BY total_jobs DESC;


-- 4. Average Company Rating by Industry

SELECT
    "Industry",
    ROUND(AVG("Rating")::NUMERIC, 2) AS average_rating
FROM "Gold_Table"
WHERE "Rating" IS NOT NULL
GROUP BY "Industry"
ORDER BY average_rating DESC;


-- 5. Jobs Available Through Easy Apply

SELECT
    "Easy Apply",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Easy Apply"
ORDER BY total_jobs DESC;


-- 6. Jobs by Company Size Category

SELECT
    "Company_Size_Category",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Company_Size_Category"
ORDER BY total_jobs DESC;


-- 7. Jobs by Company Age Category

SELECT
    "Company_Age_Category",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Company_Age_Category"
ORDER BY total_jobs DESC;


-- 8. Average Company Age by Industry

SELECT
    "Industry",
    ROUND(AVG("Company_Age")::NUMERIC, 2) AS average_company_age
FROM "Gold_Table"
WHERE "Company_Age" IS NOT NULL
GROUP BY "Industry"
ORDER BY average_company_age DESC;



-- 9. Jobs by Rating Category

SELECT
    "Rating_Category",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Rating_Category"
ORDER BY total_jobs DESC;


-- 10. Job Opportunities by Location

SELECT
    "Location",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Location"
ORDER BY total_jobs DESC
LIMIT 20;

