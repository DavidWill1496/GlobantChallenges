# GlobantChallenges
Rest API developed for the Globant challenge

## Usage
## - CREATE 
Create Bigquerytables with the endpoint createTables, method = 'GET'

    /createTable/departments
    /createTable/jobs
    /createTable/hired_employees
     
## - INSERT FROM CSV FILE
INSERT the tables from CSV format , method = 'POST'

    /insertFromCsv
     
As an input, send a json with the NAME OF THE TABLE YOU HAVE CREATED to insert and the csv path where the file is located
Example:

    {
       "table": "departments",
       "pathCsv": "https://raw.githubusercontent.com/DavidWill1496/GlobantChallenges/master/departments.csv"
    }
    
The names of the files will be: departments.csv, jobs.csv and hired_employees.csv and they are located in Github in the path:

    https://raw.githubusercontent.com/DavidWill1496/GlobantChallenges/master/

All the registers that don't fulfill the requirements (Nulls or Empty) will not be inserted but will be printed on console

## - INSERT ROW FROM JSON
Insert single registers from a json for each table, method = 'POST'

IMPORTANT!! The data id and with suffix 'id' must be number (INTEGER) otherwise it can not be inserted

To insert a new department

    Go to: /insertRow/departments
    
    And fill the json like this:
    
    {
       "id": 1,
       "department": "Product Management"
    }
    
To insert a new job:

    Go to: /insertRow/jobs
    
    Json:
    {
       "id": 182,
       "job": "Administrative Assistant IV"
    }

To insert a new hired_employee:

    Go to: /insertRow/hired_employees
    
    Json:
    {
        "id":2023,
        "name":"David Galvis",
        "datetime":"2023-04-03T08:00:00Z",
        "department_id":5,
        "job_id":94
    }
    
## MAKE A QUERY
To make a query, first go to the table you want to query , method = 'POST'

    /queryTable/departments
    /queryTable/jobs
    /queryTable/hired_employees
    
And the, send a JSON only with the 'id' you want to search

    {
        "id":1234
    }
    
And you will receive the information in a JSON like this:

    {
        "datetime": "2021-03-18 21:54:34",
        "department_id": 2,
        "id": 1234,
        "job_id": 147,
        "name": "Allys D'Enrico"
    }

## CREATE A BACKUP TABLE IN AVRO FORMAT
To create a backup in .avro format in Google Cloud Storage, method = 'GET'

    /createBackup/departments
    /createBackup/jobs
    /createBackup/hired_employees
        
You will receive a JSON response indicating when it's done, the name of the table and the path of the .avro file

For example:

    {
        "Message": "Backup .AVRO created for table",
        "Table": "departments",
        "Created_at": "gs://challenge-globant-bucket/departments.avro"
    }
    
## RESTORE A TABLE FROM BACKUP
To restore a table from the latest backup in Google Storage, use the following endpoints: , method = 'GET'

    /restoreFromBackup/departments
    /restoreFromBackup/jobs
    /restoreFromBackup/hired_employees
        
You will receive a JSON response indicating when it's done, the name of the table and the path from where the backup was taken

For example:

    {
        "Message": "Table restored from the backup",
        "Table": "departments",
        "Restored from": "gs://challenge-globant-bucket/departments.avro"
    }

## DELETE ROW FROM A TABLE
To delete a single row from a TABLE, method = 'DELETE'

    /deleteRow/departments
    /deleteRow/jobs
    /deleteRow/hired_employees
        
Then indicate the 'id' you want to delete from the table        
        
    {
        "id":1234
    }
        
You will receive a JSON response indicating when the row has been deleted

For example:

    {
        "From table": "departments",
        "Message": "Eliminated",
        "id": 13
    }
    
## DELETE A TABLE
To truncate an entire table, method = 'DELETE'

    /deleteTable/departments
    /deleteTable/jobs
    /deleteTable/hired_employees
        
You will receive a JSON response indicating the data from the table has been deleted

For example:

    {
        "Message": "The data has been eliminated",
        "From table": "departments"
    }

## CHALLENGE #2 Requirement 1
Process the 1st requirement of the Challenge 2, method = 'GET'

    /Challenge2/Requirement1
        
You will receive a HTML showing the table

## CHALLENGE #2 Requirement 2
Process the 2nd requirement of the Challenge 2, method = 'GET'

    /Challenge2/Requirement2
        
You will receive a HTML showing the table
