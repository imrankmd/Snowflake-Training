CREATE OR REPLACE TABLE MemberRecords (
    PatientID INT,
    FirstName STRING,
    LastName STRING,
    DateOfBirth DATE,
    Diagnosis STRING,
    AdmissionDate DATE,
    DischargeDate DATE
);

CREATE OR REPLACE STREAM MemberRecords_Stage_Stream 
ON TABLE MEMBERS
SHOW_INITIAL_ROWS = TRUE;


select * from MemberRecords_Stage_Stream


CREATE OR REPLACE TASK MemberRecords_Process
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = '5 MINUTE'
AS
  INSERT INTO MemberRecords (PatientID, FirstName, LastName, AdmissionDate, DischargeDate, Diagnosis)
  SELECT PatientID, FirstName, LastName, AdmissionDate, DischargeDate, Diagnosis
  FROM MemberRecords_Stage_Stream
  WHERE METADATA$ACTION = 'INSERT'  
  OR METADATA$ACTION = 'UPDATE';

ALTER TASK MemberRecords_Process RESUME;

select * from MemberRecords


SELECT COUNT(*)
FROM MEMBERS
WHERE AdmissionDate BETWEEN '2024-01-01' AND '2024-12-31';

ALTER TABLE MEMBERS 
  CLUSTER BY (AdmissionDate);

SHOW CLUSTERING INFORMATION FOR TABLE MEMBERS;


SELECT COUNT(*)
FROM MEMBERS
WHERE AdmissionDate BETWEEN '2024-01-01' AND '2024-12-31';









