-- Show general_data table & order by EmployeeID 

SELECT * 
FROM general_data
order by Employeeid

-- Count number of employees quitting

SELECT 
    Count(employeeid)
FROM general_data
where attrition = 'Yes'

-- Calculate attrition rate 

SELECT 
    (COUNT(CASE WHEN attrition = 'Yes' THEN 1 END) * 1.0 / 
    COUNT(CASE WHEN attrition = 'No' THEN 1 END)) AS attrition_rate
FROM  
    general_data

-- Join employee_survey_data table and manager_Surver_data table to get the survey information of first 10 employees

SELECT 
    e.*
    , m.JobInvolvement, m. performancerating
from general_data g
join employee_survey_data e on g.EmployeeID = e.EmployeeID
Join manager_survey_data m ON g.EmployeeID = m.EmployeeID
where g.employeeID <= 10

-- Categorize employees based on age, then count the Attrition based on age group

SELECT
	Count(employeeid)
FROM
	(SELECT
		*,
		CASE 
    		WHEn age < 25 then '<25'
        	when age >=25 and age < 34 then '25-34'
        	when age >=35 and age < 44 then '35-44'
        	when age >=45 and age < 54 then '45-54'
 			ELSE '>54'
    	End as Age_group
	from general_data)
WHERE attrition = 'Yes'
group by age_group
ORder by age_group 

-- Show top 10 employees that have the highest monthsalary

SELECT 
	*
from general_data
order by monthlyincome DESC
limit 10

-- Analyze the relationship between years at company and attrition using a CTE

WITH YearsAtCompanyCTE AS (
    SELECT 
        YearsAtCompany, 
        COUNT(EmployeeID) AS num_employees, 
        SUM(CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END) AS num_employees_quit,
        (SUM(CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END) * 1.0 / COUNT(EmployeeID)) AS attrition_rate
    FROM 
        general_data
    GROUP BY 
        YearsAtCompany
)

SELECT 
    YearsAtCompany, 
    num_employees, 
    num_employees_quit, 
    attrition_rate
FROM 
    YearsAtCompanyCTE
ORDER BY 
    YearsAtCompany;

--Combine job satisfaction and job involvement analyses using UNION

WITH CombinedAnalysisCTE AS (
    SELECT 
        'Job Satisfaction' AS category, 
        JobSatisfaction AS metric, 
        COUNT(EmployeeID) AS num_employees, 
        SUM(CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END) AS num_employees_quit,
        (SUM(CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END) * 1.0 / COUNT(EmployeeID)) AS attrition_rate
    FROM 
        general_data
    GROUP BY 
        JobSatisfaction

    UNION ALL

    SELECT 
        'Job Involvement' AS category, 
        jobinvolvement AS metric, 
        COUNT(EmployeeID) AS num_employees, 
        SUM(CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END) AS num_employees_quit,
        (SUM(CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END) * 1.0 / COUNT(EmployeeID)) AS attrition_rate
    FROM 
        general_data
    GROUP BY 
        jobinvolvement
)

SELECT 
    category, 
    metric, 
    num_employees, 
    num_employees_quit, 
    attrition_rate
FROM 
    CombinedAnalysisCTE
ORDER BY 
    category, metric;

    