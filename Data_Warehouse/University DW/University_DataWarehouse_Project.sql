-- UNIVERSITY MANAGEMENT SYSTEM DATA WAREHOUSE PROJECT

CREATE DATABASE University_Management_System;



-- CREATING TABLES

CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(100),
    building VARCHAR(100)
);

INSERT INTO departments VALUES
(1, 'Computer Science', 'Block A'),
(2, 'Electrical Engineering', 'Block B'),
(3, 'Mechanical Engineering', 'Block C'),
(4, 'Business Administration', 'Block D'),
(5, 'Mathematics', 'Block E'),
(6, 'Physics', 'Block F'),
(7, 'Chemistry', 'Block G'),
(8, 'Civil Engineering', 'Block H'),
(9, 'Psychology', 'Block I'),
(10, 'Biotechnology', 'Block J'),
(11, 'Law', 'Block K'),
(12, 'English Literature', 'Block L'),
(13, 'Economics', 'Block M'),
(14, 'Statistics', 'Block N'),
(15, 'Architecture', 'Block O'),
(16, 'Philosophy', 'Block P'),
(17, 'Sociology', 'Block Q'),
(18, 'Environmental Science', 'Block R'),
(19, 'Media Studies', 'Block S'),
(20, 'Political Science', 'Block T');

CREATE TABLE students (
    student_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    dept_id INT
);



INSERT INTO students VALUES
(1, 'Ali', 'Khan', 20, 'Male', 1),
(2, 'Sara', 'Ahmed', 21, 'Female', 4),
(3, 'Usman', 'Raza', 22, 'Male', 2),
(4, 'Ayesha', 'Malik', 19, 'Female', 1),
(5, 'Bilal', 'Sheikh', 23, 'Male', 3),
(6, 'Hina', 'Iqbal', 20, 'Female', 5),
(7, 'Hamza', 'Ali', 21, 'Male', 6),
(8, 'Fatima', 'Noor', 22, 'Female', 7),
(9, 'Zain', 'Butt', 20, 'Male', 8),
(10, 'Mariam', 'Tariq', 21, 'Female', 9),
(11, 'Omar', 'Farooq', 23, 'Male', 10),
(12, 'Noor', 'Hassan', 19, 'Female', 11),
(13, 'Daniyal', 'Abbas', 22, 'Male', 12),
(14, 'Laiba', 'Shah', 20, 'Female', 13),
(15, 'Saad', 'Mirza', 21, 'Male', 14),
(16, 'Iqra', 'Javed', 22, 'Female', 15),
(17, 'Talha', 'Siddiqui', 23, 'Male', 16),
(18, 'Mehwish', 'Aslam', 20, 'Female', 17),
(19, 'Ahmad', 'Qureshi', 21, 'Male', 18),
(20, 'Zoya', 'Nadeem', 22, 'Female', 19);


CREATE TABLE teachers (
    teacher_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    experience_years INT,
    dept_id INT
);

INSERT INTO teachers VALUES
(1, 'Dr. Kamran', 'Ali', 10, 1),
(2, 'Dr. Sana', 'Rauf', 8, 2),
(3, 'Dr. Fahad', 'Qasim', 12, 3),
(4, 'Dr. Nadia', 'Zafar', 7, 4),
(5, 'Dr. Adeel', 'Khan', 9, 5),
(6, 'Dr. Samina', 'Iqbal', 15, 6),
(7, 'Dr. Tariq', 'Mehmood', 11, 7),
(8, 'Dr. Rabia', 'Shahid', 6, 8),
(9, 'Dr. Yasir', 'Hussain', 14, 9),
(10, 'Dr. Hira', 'Butt', 5, 10),
(11, 'Dr. Salman', 'Raza', 13, 11),
(12, 'Dr. Neha', 'Jamal', 7, 12),
(13, 'Dr. Farhan', 'Asif', 10, 13),
(14, 'Dr. Amina', 'Latif', 8, 14),
(15, 'Dr. Imran', 'Chaudhry', 16, 15),
(16, 'Dr. Saba', 'Naseer', 9, 16),
(17, 'Dr. Bilal', 'Arshad', 12, 17),
(18, 'Dr. Komal', 'Younis', 6, 18),
(19, 'Dr. Asad', 'Qadir', 14, 19),
(20, 'Dr. Mahnoor', 'Saleem', 5, 20);


CREATE TABLE courses (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(100),
    credit_hours INT,
    dept_id INT
);

INSERT INTO courses VALUES
(1, 'Database Systems', 3, 1),
(2, 'Digital Logic Design', 3, 2),
(3, 'Thermodynamics', 4, 3),
(4, 'Marketing Management', 3, 4),
(5, 'Linear Algebra', 3, 5),
(6, 'Quantum Physics', 4, 6),
(7, 'Organic Chemistry', 4, 7),
(8, 'Structural Analysis', 3, 8),
(9, 'Cognitive Psychology', 3, 9),
(10, 'Genetics', 4, 10),
(11, 'Constitutional Law', 3, 11),
(12, 'Modern Poetry', 2, 12),
(13, 'Microeconomics', 3, 13),
(14, 'Probability Theory', 3, 14),
(15, 'Urban Planning', 4, 15),
(16, 'Ethics', 2, 16),
(17, 'Social Theory', 3, 17),
(18, 'Climate Change Studies', 3, 18),
(19, 'Media Ethics', 2, 19),
(20, 'International Relations', 3, 20);


CREATE TABLE classrooms (
    classroom_id INT PRIMARY KEY,
    room_number VARCHAR(10),
    capacity INT,
    building VARCHAR(100)
);

INSERT INTO classrooms VALUES
(1, 'A101', 40, 'Block A'),
(2, 'B201', 50, 'Block B'),
(3, 'C301', 45, 'Block C'),
(4, 'D401', 60, 'Block D'),
(5, 'E501', 35, 'Block E'),
(6, 'F601', 55, 'Block F'),
(7, 'G701', 40, 'Block G'),
(8, 'H801', 50, 'Block H'),
(9, 'I901', 30, 'Block I'),
(10, 'J1001', 65, 'Block J'),
(11, 'K1101', 45, 'Block K'),
(12, 'L1201', 40, 'Block L'),
(13, 'M1301', 50, 'Block M'),
(14, 'N1401', 35, 'Block N'),
(15, 'O1501', 60, 'Block O'),
(16, 'P1601', 45, 'Block P'),
(17, 'Q1701', 55, 'Block Q'),
(18, 'R1801', 50, 'Block R'),
(19, 'S1901', 40, 'Block S'),
(20, 'T2001', 65, 'Block T');


-- BRONZE LAYER (RAW DATA TABLES)

CREATE OR REPLACE TABLE university_management_system.bronze_departments
AS
SELECT * FROM departments;

CREATE OR REPLACE TABLE university_management_system.bronze_Students
AS
SELECT * FROM students;

CREATE OR REPLACE TABLE university_management_system.bronze_Teachers
AS
SELECT * FROM teachers;

CREATE OR REPLACE TABLE university_management_system.bronze_courses
AS
SELECT * FROM courses;

CREATE OR REPLACE TABLE university_management_system.bronze_classes
AS
SELECT * FROM classrooms;

-- SILVER LAYER (CLEANED AND TRANSFORMED DATA)

CREATE OR REPLACE VIEW transforming_departments
AS
SELECT DISTINCT dept_id, upper(dept_name) AS Dept_name ,building FROM bronze_departments;

CREATE OR REPLACE TABLE university_management_system.silver_departments
AS
SELECT * FROM transforming_departments;

CREATE OR REPLACE VIEW transforming_Students
AS
SELECT student_id, first_name, last_name,age,
                                      CASE 
                                        WHEN age < 20 THEN 'Under 20'
                                        WHEN age BETWEEN 20 AND 25 THEN '20-25'
                                        WHEN age BETWEEN 26 AND 30 THEN '26-30'
                                        ELSE 'Over 30'
                                        END AS age_group,
                                      gender,
                                      dept_id FROM bronze_Students;

CREATE OR REPLACE TABLE silver_students
AS
SELECT * FROM transforming_Students;

CREATE OR REPLACE VIEW transforming_teachers
AS
SELECT teacher_id,upper(first_name) AS First_name ,last_name,experience_years,
                                  Rank() OVER(order by experience_years desc) AS rank,
                                  dept_id FROM bronze_teachers;

CREATE OR REPLACE TABLE silver_teachers
AS
SELECT * FROM transforming_teachers;


CREATE OR REPLACE VIEW transforming_courses
AS
SELECT course_id,course_name,credit_hours,
                            CASE WHEN credit_hours < 3 THEN "Low_Load"
                                 WHEN credit_hours BETWEEN 3 AND 5 THEN "Medium_Load"
                                 WHEN credit_hours > 5 THEN "High_Load"
                                 ELSE "Unknown" 
                            END AS load,
                            dept_id FROM bronze_courses;

CREATE OR REPLACE TABLE silver_courses
AS
SELECT * FROM transforming_courses;


CREATE OR REPLACE VIEW transforming_classes
AS
SELECT 
    classroom_id,room_number,capacity,building,
    CASE 
        WHEN capacity <= 35 THEN 'Small'
        WHEN capacity BETWEEN 36 AND 50 THEN 'Medium'
        ELSE 'Large'
            END AS room_size
            FROM classrooms;


CREATE OR REPLACE TABLE silver_classes
AS
SELECT * FROM transforming_classes;



CREATE OR REPLACE TABLE university_management_system.dim_departments(
    dept_id INT,
    dept_key STRING,
    dept_name STRING,
    building STRING
)

CREATE OR REPLACE VIEW university_management_system.transforming_dim_departments 
AS
SELECT
    dept_id,
    row_number() over(order by dept_id) as dept_key,
    dept_name,
    building
FROM
    silver_departments

INSERT INTO university_management_system.dim_departments
SELECT * FROM transforming_dim_departments

-- DIMENSION TABLES (STAR SCHEMA)

CREATE TABLE university_management_system.dim_teacher(
    teacher_key INT,
    teacher_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    experience_years INT,
    dept_id INT
);

CREATE VIEW university_management_system.transforming_dim_teachers
AS
SELECT row_number() over(order by teacher_id) as teacher_key,
    teacher_id,
    first_name,
    last_name,
    experience_years,
    dept_id
FROM silver_teachers

INSERT INTO university_management_system.dim_teacher
SELECT * FROM transforming_dim_teachers


CREATE  TABLE university_management_system.transforming_dim_students (
    student_key INT,
    student_id INT,
    first_name STRING,
    last_name STRING,
    age INT,
    gender STRING,  
    dept_id INT
);


CREATE OR REPLACE VIEW university_management_system.transforming_dim_students 
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY student_id) AS student_key,
    student_id,
    first_name,
    last_name,
    age,
    gender,
    dept_id
FROM silver_students;


INSERT INTO university_management_system.dim_students
SELECT * FROM transforming_dim_students


CREATE TABLE university_management_system.dim_courses (
    course_key INT,
    course_id INT ,
    course_name VARCHAR(100),
    credit_hours INT,
    dept_id INT
);


CREATE OR REPLACE VIEW university_management_system.transforming_dim_courses 
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY course_id) AS course_key,
    course_id,
    course_name,
    credit_hours,
    dept_id
FROM silver_courses;

INSERT INTO university_management_system.dim_courses
SELECT * FROM transforming_dim_courses



CREATE TABLE university_management_system.dim_classes (
    class_key INT,
    classroom_id INT ,
    room_number VARCHAR(10),
    capacity INT,
    building VARCHAR(100)
);

CREATE VIEW university_management_system.transforming_dim_classes
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY classroom_id) AS class_key,
    classroom_id,
    room_number,
    capacity,
    building
FROM silver_classes;

INSERT INTO university_management_system.dim_classes
SELECT * FROM transforming_dim_classes


CREATE TABLE university_management_system.fact_table (
    fact_key INT,
    student_key INT,
    course_key INT,
    teacher_key INT,
    dept_key INT,
    classroom_key INT,
    credit_hours INT,
    experience_years INT
);


-- FACT TABLE


INSERT INTO university_management_system.fact_table
SELECT
    ROW_NUMBER() OVER (ORDER BY s.student_key, c.course_key) AS fact_key,
    s.student_key,
    c.course_key,
    t.teacher_key,
    d.dept_key,
    cl.class_key,
    c.credit_hours,
    t.experience_years
FROM university_management_system.dim_students s
JOIN university_management_system.dim_courses c
    ON s.dept_id = c.dept_id
JOIN university_management_system.dim_teacher t
    ON s.dept_id = t.dept_id
JOIN university_management_system.dim_departments d
    ON s.dept_id = d.dept_id
JOIN university_management_system.dim_classes cl
    ON d.building = cl.building;


-- ANALYTICAL QUERIES

-- Teachers Handling the Highest Number of Courses

SELECT 
t.first_name,
t.last_name,
COUNT(f.course_key) AS total_courses
FROM university_management_system.fact_table f
JOIN university_management_system.dim_teacher t
ON f.teacher_key = t.teacher_key
GROUP BY t.first_name, t.last_name
ORDER BY total_courses DESC;

-- Shows teachers with the highest years of experience

SELECT 
t.first_name,
t.last_name,
MAX(f.experience_years) AS experience
FROM university_management_system.fact_table f
JOIN university_management_system.dim_teacher t
ON f.teacher_key = t.teacher_key
GROUP BY t.first_name, t.last_name
ORDER BY experience DESC;

-- Total Courses Offered by Each Department

SELECT 
d.dept_name,
COUNT(f.course_key) AS total_courses
FROM university_management_system.fact_table f
JOIN university_management_system.dim_departments d
ON f.dept_key = d.dept_key
GROUP BY d.dept_name;


