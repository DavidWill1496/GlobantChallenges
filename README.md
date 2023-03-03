# GlobantChallenges
Rest API developed for the Globant challenge

## Usage
## - CREATE 
Create Bigquerytables with the endpoint createTables, method = 'GET'

    /createTable/departments
    /createTable/jobs
    /createTable/hired_employees
Expect a Json response when the table is created

    {
       "Message": "Table created successfully",
       "Table": "departments"
    }
     
INSERT the tables from CSV format with the endpoint createTables, method = 'POST'

    /insertFromCsv
    
    {
    
Using POST method - 

    curl http://127.0.0.1:5000 -d "q='Text String to check Sentiment.'"
    
or use in a web based form and send POST request.
