CREATE OR REPLACE TABLE Member_Info (
    PatientID INT,
    FirstName STRING,
    LastName STRING,
    DateOfBirth DATE,
    Diagnosis STRING,
    AdmissionDate DATE,
    DischargeDate DATE
);

CREATE FILE FORMAT my_csv_format TYPE=CSV
FIELD_OPTIONALLY_ENCLOSED_BY='"';

CREATE OR REPLACE STAGE Member_Info_Stage
URL='s3://test/Member_Info.csv'
CREDENTIALS=(AWS_KEY_ID='xxxxxxx' AWS_SECRET_KEY='xxxxxxxxx');


COPY INTO Member_Info
FROM @Member_Info_Stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

CREATE MATERIALIZED VIEW MemberByDiagnosis AS
SELECT Diagnosis,
       AVG(DATEDIFF(DAY, AdmissionDate, DischargeDate)) AS AvgStay
FROM Member_Info
GROUP BY Diagnosis;

SELECT * FROM MemberByDiagnosis;