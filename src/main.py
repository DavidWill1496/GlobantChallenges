import pandas as pd
import webbrowser

from flask import Flask, jsonify, request
from config import config
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from pandasql import sqldf

app = Flask(__name__)

#key_path = "env/BigQuery/challengeglobant-7559939a117e.json"
project_id = "challengeglobant"
dataset = "DataSet_Challenge"
#credentials = service_account.Credentials.from_service_account_file(
#    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
#client = bigquery.Client(credentials=credentials, project=project_id)
client = bigquery.Client()

# Data rules for table departments
columns_departments = ['id', 'department']
schema_departments = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("department", "STRING", mode="REQUIRED")]

# Data rules for table jobs
columns_jobs = ['id', 'job']
schema_jobs = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("job", "STRING", mode="REQUIRED"),]

# Data rules for table hired_employees
columns_hired_employees = ['id', 'name', 'datetime', 'department_id', 'job_id']
schema_hired_employees = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("datetime", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("department_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("job_id", "INTEGER", mode="REQUIRED"),]

@app.route('/queryTable/<gettable>', methods=['POST'])
def queryTable(gettable):
    try:
        id = request.json['id']
        sql = """SELECT * FROM `challengeglobant.DataSet_Challenge.{}`
                WHERE id={} """.format(gettable, id)
        query_job = client.query(sql)
        for row in query_job:
            print(row)
        if gettable == "departments":
            return jsonify({'id': row[0], 'department': row[1]})
        elif gettable == "jobs":
            return jsonify({'id': row[0], 'job': row[1]})
        elif gettable == "hired_employees":
            return jsonify({'id': row[0], 'name': row[1], 'datetime': row[2], 'department_id': row[3], 'job_id': row[4]})
        else:
            return jsonify({'Message': "Error while reading the table"})
    except Exception as ex:
        raise ex

@app.route('/deleteRow/<gettable>', methods=['DELETE'])
def deleteRow(gettable):
    try:
        id = request.json['id']
        sql = """DELETE FROM `challengeglobant.DataSet_Challenge.{}`
                WHERE id={} """.format(gettable, id)
        query_job = client.query(sql)
        query_job.result()
        return jsonify({'Message': "Eliminated", "id":id, "From table":gettable})
    except Exception as ex:
        raise ex

@app.route('/deleteTable/<gettable>', methods=['DELETE'])
def deleteTable(gettable):
    try:
        sql = """TRUNCATE TABLE `challengeglobant.DataSet_Challenge.{}` """.format(gettable)
        query_job = client.query(sql)
        query_job.result()
        return jsonify({'Message': "The data has been eliminated", "From table":gettable})
    except Exception as ex:
        raise ex

@app.route('/createBackup/<gettable>')
def backup(gettable):
    dataset_id = 'DataSet_Challenge'
    bucket_name = 'challenge-globant-bucket'
    try:
        file_name = '{}.avro'.format(gettable)
        destination_uri = 'gs://{}/{}'.format(bucket_name, file_name)
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        table_ref = dataset_ref.table(gettable)
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.AVRO
        extract_job = client.extract_table(
           table_ref,
           destination_uri,
           job_config=job_config,
           location="southamerica-east1",)
        extract_job.result()
        return jsonify({'Message': "Backup .AVRO created for table", "Table":gettable, "Created at":destination_uri})
    except Exception as ex:
        print(ex)
        return jsonify({'Message': "Error creating the backup", "table":gettable})

@app.route('/restoreFromBackup/<gettable>')
def restore(gettable):
    dataset_id = 'DataSet_Challenge'
    bucket_name = 'challenge-globant-bucket'
    try:
        file_name = '{}.avro'.format(gettable)
        table_id = "{}.{}.{}".format(project_id, dataset_id, gettable)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.AVRO,)
        uri = 'gs://{}/{}'.format(bucket_name, file_name)
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config)
        load_job.result()
        return jsonify({'1) Message': "Table restored from the backup", "2) Table":gettable, "3) Restored from":uri})
    except Exception as ex:
        print(ex)
        return jsonify({'message': "Failed to restore from backup", "table":gettable})

def querySelectAll(table):
    select = "SELECT * FROM `challengeglobant.DataSet_Challenge.{}`".format(table)
    return select

def reqChallengeTwo():
    dataset = "DataSet_Challenge"
    tableOne = 'requirementOne'
    schemaReqOne = [
        bigquery.SchemaField("department", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("job", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Q", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("count_col", "INTEGER", mode="REQUIRED")]
    
    query_departments = client.query(querySelectAll("departments"))
    dfDepartments = query_departments.to_dataframe()
    dfDepartments = dfDepartments.drop_duplicates()

    query_jobs = client.query(querySelectAll("jobs"))
    dfJobs = query_jobs.to_dataframe()
    dfJobs = dfJobs.drop_duplicates()

    query_employees = client.query(querySelectAll("hired_employees"))
    dfEmployees = query_employees.to_dataframe()
    dfEmployees = dfEmployees.drop_duplicates()

    dfJoinEmpDep = dfEmployees.merge(dfDepartments, left_on=['department_id'], right_on = ['id'], how="left")
    dfJoinAllDfs = dfJoinEmpDep.merge(dfJobs, left_on=["job_id"], right_on=['id'], how="left")
    dfJoinAllDfs['Q'] = dfJoinAllDfs.apply(f,axis=1)
    dfQ = dfJoinAllDfs[['id_y', 'department', 'job', 'Q']]

    queryCount = """ SELECT id_y AS 'id', department, job, Q, COUNT(*) AS "count_col"
                   FROM dfQ 
                   GROUP BY department, job, Q """
    dfCount = sqldf(queryCount)

    createDatasetBQ(dataset)
    createTableBQ(schemaReqOne, dataset, tableOne)
    dec_if_exists = 'replace'
    dfToGbq(dataset,tableOne,dfCount,dec_if_exists)

    queryPivot = """ SELECT id, department, job, IFNULL(Q1, 0) as Q1, IFNULL(Q2, 0) as Q2, IFNULL(Q3, 0) as Q3, IFNULL(Q4, 0) as Q4 FROM
                    (SELECT id, department, job, Q, count_col  FROM `challengeglobant.DataSet_Challenge.requirementOne`)
                    PIVOT (SUM(count_col) FOR Q IN ('Q1', 'Q2', 'Q3', 'Q4')) """
    selectPivot = client.query(queryPivot)
    dfPivot = selectPivot.to_dataframe()
    return dfPivot

def dfToGbq(dataset,table,df,dec_if_exists):
    table_id = "{}.{}.{}".format(project_id, dataset, table)
    destination = dataset + '.' + table
    tableFromBQ = client.get_table(table_id)
    generated_schema = [{'name': i.name, 'type': i.field_type}
                        for i in tableFromBQ.schema]
    df.to_gbq(destination_table=destination,
              project_id=project_id,
              table_schema=generated_schema,
              if_exists=dec_if_exists)
    return "Df converted to BigQuery"

@app.route('/Challenge2/Requirement1')
def getRequirementOne():
    try:
        dfReqOne = reqChallengeTwo()
        print(dfReqOne)
        dfReqOne.to_html('output1.html')
        webbrowser.open('output1.html')
        dfJson = dfReqOne.to_json(orient ='records')
        return dfJson
    except Exception as ex:
        print(Exception, ex)
        return jsonify({'Message': "Error"})

@app.route('/Challenge2/Requirement2')
def getRequirementTwo():
    try:
        df = reqChallengeTwo()
        df['hired'] = df['Q1'] + df['Q2'] + df['Q3'] + df['Q4']
        dfAgg = df.groupby('department')['hired'].sum().reset_index()

        query_departments = client.query(querySelectAll("departments"))
        dfDepartments = query_departments.to_dataframe()
        dfDepartments = dfDepartments.drop_duplicates()

        dfJoinAggDep = dfAgg.merge(dfDepartments, left_on=['department'], right_on = ['department'], how="inner")
        dfReqTwo = dfJoinAggDep[['id', 'department', 'hired']]
        dfOrdered = dfReqTwo.sort_values('hired',ascending=False)
        mean = dfOrdered["hired"].mean()
        print(mean)
        dfReq2 = dfOrdered[dfOrdered.hired > mean] # Select only the rows that fulfill the condition for column 'hired'
        print(dfReq2)
 
        dfReq2.to_html('output2.html')
        webbrowser.open('output2.html')
        dfJson = dfReq2.to_json(orient ='records')
        return dfJson
    except Exception as ex:
        print(Exception, ex)
        return jsonify({'Message': "Error"})

# Calculates the Q by date range
def f(row):
    if '2021-01-31 00:00:00' <= row['datetime'] <= '2021-03-31 23:59:59':
        val = 'Q1'
    elif '2021-04-01 00:00:00' <= row['datetime'] <= '2021-06-30 23:59:59':
        val = 'Q2'
    elif '2021-07-01 00:00:00' <= row['datetime'] <= '2021-09-30 23:59:59':
        val = 'Q3'
    elif '2021-10-01 00:00:00' <= row['datetime'] <= '2021-12-31 23:59:59':
        val = 'Q4'
    else:
        val = 'Not in 2021'
    return val

def getCsv(csvPath, columns):
    df = pd.read_csv(csvPath, engine="pyarrow",
                     header=None, names=columns, sep=',')
    return df

@app.route('/insertRow/departments', methods=['POST'])
def insertRowsDepartments():
    id = request.json['id']
    department = request.json['department']
    try:
        sql = """INSERT INTO `challengeglobant.DataSet_Challenge.departments`(id, department)
        VALUES ({}, '{}')""" .format(id, department)
        query_job = client.query(sql)
        query_job.result()
        return jsonify({'Message': "Rows inserted correctly", "table": "departments"})
    except Exception as ex:
        print(ex)
        return jsonify({'Message': "Error while inserting rows"})

@app.route('/insertRow/jobs', methods=['POST'])
def insertRowsJobs():
    id = request.json['id']
    job = request.json['job']
    try:
        sql = """INSERT INTO challengeglobant.DataSet_Challenge.jobs (id, job)
        VALUES ({}, '{}')""" .format(id, job)
        query_job = client.query(sql)
        query_job.result()
        return jsonify({'Message': "Rows inserted correctly", "table": "jobs"})
    except Exception as ex:
        print(ex)
        return jsonify({'Message': "Error while inserting rows"})

@app.route('/insertRow/hired_employees', methods=['POST'])
def insertRowsHE():
    id = request.json['id']
    name = request.json['name']
    datetime = request.json['datetime']
    department_id = request.json['department_id']
    job_id = request.json['job_id']
    try:
        sql = """INSERT INTO challengeglobant.DataSet_Challenge.hired_employees (id, name, datetime, department_id, job_id)
        VALUES ({}, '{}', '{}', {}, {})""" .format(id, name, datetime, department_id,job_id)
        query_job = client.query(sql)
        query_job.result()
        return jsonify({'Message': "Rows inserted correctly", "table": "hired_employees"})
    except Exception as ex:
        print(ex)
        return jsonify({'Message': "Error while inserting rows"})

@app.route('/insertFromCsv', methods=['POST'])
def insertFromCsv():
    gettable = request.json['table']
    csvPath = request.json['pathCsv']
    # csvPath = "https://raw.githubusercontent.com/DavidWill1496/GlobantChallenges/master/departments.csv"
    try:
        if gettable == "hired_employees":
            df = getCsv(csvPath, columns_hired_employees)
            # Selected rows that have missing values and do not accomplish the need
            selected_rows = df[df.isnull().any(axis=1)]
            df = df.dropna(axis=0)
            df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df['department_id'] = df['department_id'].astype(int)
            df['job_id'] = df['job_id'].astype(int)
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            dfFinal = df.dropna(axis=0)
            dfFinal = dfFinal.drop_duplicates()
            dec_if_exists='replace'
            dfToGbq(dataset,gettable,dfFinal,dec_if_exists)
            print("\n ===== Transactions that didn't accomplish the requirements for {}".format("departments"))
            print(selected_rows)
            return jsonify({'Message': "Rows inserted correctly", "table": gettable})
        elif gettable == "departments":
            df = getCsv(csvPath, columns_departments)
            # Selected rows that have missing values and do not accomplish the need
            selected_rows = df[df.isnull().any(axis=1)]
            df = df.dropna(axis=0)
            df['id'] = df['id'].astype(int)
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            dfFinal = df.dropna(axis=0)
            dfFinal = dfFinal.drop_duplicates()
            dec_if_exists='replace'
            dfToGbq(dataset,gettable,dfFinal,dec_if_exists)
            print("\n ===== Transactions that didn't accomplish the requirements")
            print(selected_rows)
            return jsonify({'Message': "Rows inserted correctly", "table": gettable})
        elif gettable == "jobs":
            df = getCsv(csvPath, columns_jobs)
            # Selected rows that have missing values and do not accomplish the need
            selected_rows = df[df.isnull().any(axis=1)]
            df = df.dropna(axis=0)
            df['id'] = df['id'].astype(int)
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            dfFinal = df.dropna(axis=0)
            dfFinal = dfFinal.drop_duplicates()
            dec_if_exists='replace'
            dfToGbq(dataset,gettable,dfFinal,dec_if_exists)
            print("\n ===== Transactions that didn't accomplish the requirements")
            print(selected_rows)
            return jsonify({'Message': "Rows inserted correctly", "table": gettable})
    except Exception as ex:
        print(ex)
        return jsonify({'Message': "Error while inserting rows"})

def createDatasetBQ(dataset):
    dataset_ref = client.dataset(dataset)
    try:
        dataset = client.get_dataset(dataset_ref)
        print('Dataset {} already exists.'.format(dataset))
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        dataset = client.create_dataset(dataset)
    return dataset


def createTableBQ(schemaTable, dataset, table_id):
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table_id)
    try:
        table = client.get_table(table_ref)
        print('table {} already exists.'.format(table))
        return jsonify({'Message': "Table already exists"})
    except NotFound:
        table = bigquery.Table(table_ref, schemaTable)
        table = client.create_table(table)
        print('table {} created successfully.'.format(table.table_id))
        return jsonify({'Message': "Table created successfully"})
    # return table

# Create the table in BigQuery for the first time
@app.route('/createTable/<gettablecreate>')
def createTable(gettablecreate):
    try:
        dataset = "DataSet_Challenge"
        if gettablecreate == 'departments':
            createDatasetBQ(dataset)
            createTableBQ(schema_departments, dataset, gettablecreate)
            return jsonify({'Message': "Table created successfully", "Table": gettablecreate})
        elif gettablecreate == 'hired_employees':
            createDatasetBQ(dataset)
            createTableBQ(schema_hired_employees, dataset, gettablecreate)
            return jsonify({'Message': "Table created successfully", "Table": gettablecreate})
        elif gettablecreate == 'jobs':
            createDatasetBQ(dataset)
            createTableBQ(schema_jobs, dataset, gettablecreate)
            return jsonify({'Message': "Table created successfully", "Table": gettablecreate})
    except Exception as ex:
        return jsonify({'Message': "Error while creating table"}), 404

def PageNotFound(error):
    return "<h1>The website you are looking for doesn't exist</h1>", 404

@app.route('/')
def selectAction():
    return """
    <h1>Main page of the the Rest API for Globant Challenges</h1><br>
    <h2>See all the different endpoints in the README.md file</h2>
    """

if __name__ == '__main__':
    app.config.from_object(config['development'])
    app.register_error_handler(404, PageNotFound)  # Call the error
    app.run()
