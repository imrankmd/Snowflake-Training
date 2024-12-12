
//Task 1

CREATE OR REPLACE TABLE Patients (
    PatientID INT,
    FirstName STRING,
    LastName STRING,
    DateOfBirth DATE,
    Diagnosis STRING,
    AdmissionDate DATE,
    DischargeDate DATE,
    Region STRING  -- Add this column to store the region information for each patient
);

INSERT INTO Patients (PatientID, FirstName, LastName, DateOfBirth, Diagnosis, AdmissionDate, DischargeDate, Region)
VALUES
    (1, 'John', 'Doe', '1985-06-15', 'Flu', '2024-12-01', '2024-12-05', 'North'),
    (2, 'Jane', 'Smith', '1990-08-22', 'Covid-19', '2024-12-02', '2024-12-10', 'South'),
    (3, 'Mike', 'Brown', '1975-03-30', 'Asthma', '2024-12-03', '2024-12-08', 'North'),
    (4, 'Emily', 'Davis', '2000-01-15', 'Fever', '2024-12-04', '2024-12-06', 'East');


CREATE OR REPLACE ROW ACCESS POLICY region_policy
AS (user_region STRING, data_region STRING) RETURNS BOOLEAN ->
  user_region = data_region;
  
ALTER TABLE Patients
ADD ROW ACCESS POLICY region_policy on (Region);


SET user_region = 'North'; 

SELECT * FROM Patients;



// Task 2


CREATE OR REPLACE TABLE PatientRecords (
    PatientID INT,
    Name STRING,
    Diagnosis STRING,
    AdmissionDate DATE
);

INSERT INTO PatientRecords VALUES
(1, 'Alice', 'Flu', '2024-11-01'),
(2, 'Bob', 'Cold', '2024-11-02'),
(3, 'Charlie', 'Asthma', '2024-11-03');

UPDATE PatientRecords
SET Diagnosis = 'Pneumonia'
WHERE PatientID = 1;

CREATE OR REPLACE TABLE PatientRecordsClone
CLONE PatientRecords
AT (offset => -60);

-- Query the cloned table
SELECT * FROM PatientRecordsClone;

-- Query the live production table
SELECT * FROM PatientRecords;