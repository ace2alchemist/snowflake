# Query Snowflake Data Using Python For Business Users

## Use Case :
This script will help users to connect to snowflake and execute custom queries without logging into snowflake UI. It was developed as I wanted some users to execute their queries from some Azure batch and from their local console. The use case would be limiting their access to some specific schema or any sandbox schem and let them playaround over there without letting them view any other data. Also it can be used to compare the results of the same tables between snowflake and synapse


## Functions:
* **Connect to Snowflake**: Establish a connection to Snowflake using provided credentials.
* **Execute Query**: Take user input (custom SQL query) and execute it against Snowflake.
* **Parameterized Queries**: Parse user-defined parameters and inject them into the SQL query before execution.
* **Fetch Results**: Retrieve query results from Snowflake and format them into a usable structure (e.g., DataFrame, CSV).
* **Error Handling**: Catch and handle exceptions during connection establishment and query execution, providing informative feedback to users.

## Benefits:
* **Ease of Use**: Simplifies the process of accessing Snowflake data for business users without requiring deep technical knowledge of SQL or Snowflake's backend.
* **Customizability**: Allows users to tailor queries to their specific needs, enabling ad-hoc analysis and reporting.
* **Time Savings**: Automates the process of querying Snowflake, reducing manual effort and increasing productivity.
* **Data Integrity**: Ensures data consistency and accuracy by providing direct access to Snowflake's centralized data repository.

# Python Code :

```
import snowflake.connector as sf
import pandas as pd

class Snowflake_Config:
    def __init__(self,authenticator, username, account, warehouse, role, database, schema, object):
        self.authenticator = authenticator
        self.username = username
        self.account = account
        self.warehouse = warehouse
        self.role = role
        self.database = database
        self.schema = schema
        self.object = object

    def get_authenticator(self):
        return self.authenticator
   
    def set_authenticator(self, authenticator):
        self.authenticator = authenticator

    def get_username(self):
        return self.username

    def set_username(self, username):
        self.username = username

    def get_account(self):
        return self.account

    def set_account(self, account):
        self.account = account

    def get_warehouse(self):
        return self.warehouse

    def set_warehouse(self, warehouse):
        self.warehouse = warehouse

    def get_role(self):
        return self.role

    def set_role(self, role):
        self.role = role

    def get_database(self):
        return self.database

    def set_database(self, database):
        self.database = database

    def get_schema(self):
        return self.schema

    def set_schema(self, schema):
        self.schema = schema

    def get_object(self):
        return self.object

    def set_role(self, object):
        self.table = object


def create_connection(config: Snowflake_Config):
        try:
            conn = sf.connect(
                authenticator=config.get_authenticator(),
                user=config.get_username(),
                account=config.get_account(),
                warehouse=config.get_warehouse(),
                database=config.get_database(),
                schema=config.get_schema(),
                role=config.get_role(),
            )
            print('Snowflake Connection established!')
            return conn
        except Exception as e:
            print('Connection Failed. Please try again.')
            print('Error: ' + str(e))
            quit()

class SnowflakeQueryExecutor:
    def __init__(self, config):
        self.config = config

    def execute_query(self, query):
        conn = create_connection(self.config)
        try:
            df = pd.read_sql_query(query, conn)
            print('Snowflake Dataframe Load Successful.')
            print(df)
            for col in df.columns:
                print(col)
            return df
        except Exception as e:
            print('Snowflake Dataframe load Unsuccessful. Please try again.')
            print('Error: ' + str(e))

def main():
    # Define configuration
    config = Snowflake_Config(
        authenticator='externalbrowser',
        username='<username here>',
        account= '<snowflake account name>',
        warehouse='<warehouse name>',
        role='<snowflake role here>',
        database='<snowflake db name>',
        schema='<snowflake schema name>',
        object='<snowflake object name>'

    )

    # Define the query
    sf_qry = f"SELECT * FROM {config.get_database()}.{config.get_schema()}.{config.get_object()} LIMIT 10"

    # Execute the query
    executor = SnowflakeQueryExecutor(config)
    sf_df = executor.execute_query(sf_qry)

if __name__ == "__main__":
    main()

```

