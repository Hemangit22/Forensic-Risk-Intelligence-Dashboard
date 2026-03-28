-- ==========================================================
-- PHASE 2: SQL FORENSIC ANALYSIS 
-- PROJECT: MITIGATION MODEL DRIFT 
-- ==========================================================

-- 1. Setup Environment
CREATE DATABASE IF NOT EXISTS Risk_Project;
USE Risk_Project;

-- 2. Build the Table with your exact 45 columns
CREATE TABLE Risk_Intelligence_Data (
    fraud_bool TEXT, income TEXT, name_email_similarity TEXT, prev_address_months_count TEXT, 
    current_address_months_count TEXT, customer_age TEXT, days_since_request TEXT, 
    intended_balcon_amount TEXT, payment_type TEXT, zip_count_4w TEXT, velocity_6h TEXT, 
    velocity_24h TEXT, velocity_4w TEXT, bank_branch_count_8w TEXT, 
    date_of_birth_distinct_emails_4w TEXT, employment_status TEXT, credit_risk_score TEXT, 
    email_is_free TEXT, housing_status TEXT, phone_home_valid TEXT, phone_mobile_valid TEXT, 
    bank_months_count TEXT, has_other_cards TEXT, proposed_credit_limit TEXT, 
    foreign_request TEXT, source_1 TEXT, session_length_in_minutes TEXT, device_os TEXT, 
    keep_alive_session TEXT, device_distinct_emails_8w TEXT, device_fraud_count TEXT, 
    month_1 TEXT, Report_Date TEXT, Year TEXT, Firm_Name TEXT, Total_Audit_Engagements TEXT, 
    High_Risk_Cases TEXT, Compliance_Violations TEXT, Fraud_Cases_Detected TEXT, 
    Industry_Affected TEXT, Total_Revenue_Impact TEXT, AI_Used_for_Auditing TEXT, 
    Employee_Workload TEXT, Audit_Effectiveness_Score TEXT, Client_Satisfaction_Score TEXT
);

-- 3. The High-Speed Import
-- Make sure the file is still in: C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FinalRisk_Intelligence_Data.csv'
INTO TABLE Risk_Intelligence_Data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS;

-- Matching Columns Datatype 
USE Risk_Project;
ALTER TABLE Risk_Intelligence_Data 
-- Boolean (0 or 1)
MODIFY COLUMN fraud_bool TINYINT,
-- Decimals and Floats
MODIFY COLUMN income DECIMAL(10,2),
MODIFY COLUMN name_email_similarity FLOAT,
-- Integers
MODIFY COLUMN prev_address_months_count INT,
MODIFY COLUMN current_address_months_count INT,
MODIFY COLUMN customer_age INT,
-- Scientific/Long Decimals
MODIFY COLUMN days_since_request DOUBLE,
MODIFY COLUMN intended_balcon_amount DOUBLE,
-- Counts and Velocities
MODIFY COLUMN zip_count_4w INT,
MODIFY COLUMN velocity_6h FLOAT,
MODIFY COLUMN velocity_24h FLOAT,
MODIFY COLUMN velocity_4w FLOAT,
MODIFY COLUMN bank_branch_count_8w INT,
MODIFY COLUMN date_of_birth_distinct_emails_4w INT,
MODIFY COLUMN credit_risk_score INT,
-- Binary/Flags
MODIFY COLUMN phone_home_valid TINYINT,
MODIFY COLUMN phone_mobile_valid TINYINT,
MODIFY COLUMN has_other_cards TINYINT,
MODIFY COLUMN foreign_request TINYINT,
-- Dates and Years
MODIFY COLUMN Report_Date DATE,
MODIFY COLUMN Year INT,
-- Business Metrics
MODIFY COLUMN Total_Audit_Engagements INT,
MODIFY COLUMN High_Risk_Cases INT,
MODIFY COLUMN Compliance_Violations INT,
MODIFY COLUMN Fraud_Cases_Detected INT,
MODIFY COLUMN Total_Revenue_Impact DECIMAL(15,2),
MODIFY COLUMN Employee_Workload INT,
MODIFY COLUMN Audit_Effectiveness_Score FLOAT,
MODIFY COLUMN Client_Satisfaction_Score FLOAT;

-- 4. Data Integrity Audit (The "Reconciliation" Step)
-- Confirms the 1,000,000 record 'Census' matches Alteryx output
SELECT COUNT(*) AS total_records_check FROM Risk_Intelligence_Data;

-- 5. Data Governance Check (Checking for NULLs in critical flags)
-- Ensures the Alteryx 'Data Cleansing' step was successful
SELECT COUNT(*) AS null_fraud_flags 
FROM Risk_Intelligence_Data 
WHERE fraud_bool IS NULL;

-- 4. Audit Procedure 
-- A: Pulse Check (Model Drift) Monitoring if fraud is rising over the timeline
SELECT 
    Report_Date,
    COUNT(*) AS total_applications,
    SUM(fraud_bool) AS total_fraud_cases,
    ROUND(AVG(fraud_bool) * 100, 2) AS fraud_rate_percentage
FROM Risk_Intelligence_Data
GROUP BY Report_Date
ORDER BY Report_Date ASC;

-- B. Gap Analysis: Finding fraud missed by traditional low-risk credit scores
SELECT income, fraud_bool, credit_risk_score
FROM Risk_Intelligence_Data
WHERE fraud_bool = 1 AND credit_risk_score < 300; 

-- C. Velocity Monitor: Identifying devices with too many account attempts
-- Note: 'device_distinct_emails_8w' matches your schema from Alteryx
SELECT device_distinct_emails_8w, COUNT(*) AS volume, SUM(fraud_bool) AS fraud_count
FROM Risk_Intelligence_Data
GROUP BY device_distinct_emails_8w
HAVING volume > 5;

-- D.Feature Correlation: THE "WHY"
-- Identifying the 'High-Risk' address history threshold
SELECT 
    prev_address_months_count, 
    COUNT(*) AS total_apps, 
    SUM(CAST(fraud_bool AS SIGNED)) AS fraud_count,
    ROUND(AVG(CAST(fraud_bool AS SIGNED)) * 100, 2) AS fraud_percent
FROM Risk_Intelligence_Data
GROUP BY prev_address_months_count
ORDER BY fraud_percent DESC
LIMIT 10;

-- E. Regulatory Compliance: EU AI Act Bias Detection (Checking Income Brackets)
SELECT 
    CASE 
        WHEN income < 0.2 THEN 'Low Income'
        WHEN income BETWEEN 0.2 AND 0.6 THEN 'Mid Income'
        ELSE 'High Income' 
    END AS Income_Bracket,
    AVG(fraud_bool) AS Fraud_Rate_By_Group
FROM Risk_Intelligence_Data
GROUP BY 1; 

-- F. TABLEAU OPTIMIZATION LAYER (View for instant dashboard loading)
CREATE OR REPLACE VIEW v_dashboard_final AS
SELECT 
    Report_Date,
    CASE 
        WHEN income < 0.2 THEN 'Low Income'
        WHEN income BETWEEN 0.2 AND 0.6 THEN 'Mid Income'
        ELSE 'High Income' 
    END AS Income_Bracket,
    COUNT(*) AS Total_Applications,
    SUM(fraud_bool) AS Total_Fraud_Detected,
    AVG(credit_risk_score) AS Avg_Credit_Risk
FROM Risk_Intelligence_Data
GROUP BY Report_Date, Income_Bracket; 

SELECT * FROM v_dashboard_final;