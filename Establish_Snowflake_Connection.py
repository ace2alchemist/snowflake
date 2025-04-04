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
