
import pandas as pd
import mysql.connector as mc
from dagster import OutputContext, InputContext, IOManager


class MySQL_IOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def create_connection(self):
        conn = mc.connect(
            host=self._config["host"],
            user=self._config["user"],
            password=self._config["password"],
            database=self._config["database"]
        )
        try:
            return conn
        except mc.Error as err:
            raise ValueError(f'ERROR: {err}')

    @staticmethod
    def create_table_if_not_exists(connection, table_name, col_types,
                                   context, primary_key=None):
        cursor = connection.cursor()
        # Define columns
        columns = [f"{name} {col_type}" for col_dict in col_types for name, col_type in col_dict.items()]
        # Define table creation query
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)}"
        if primary_key:
            create_table_query += f", PRIMARY KEY ({', '.join(primary_key)})"
        create_table_query += ");"
        try:
            cursor.execute(create_table_query)
            context.log.info(f"Table {table_name} created successfully.")
        except mc.Error as err:
            context.log.info(f"Error: {err}")
        finally:
            cursor.close()

    @staticmethod
    def drop_table_if_not_exists(connection, table_name, context):
        cursor = connection.cursor()
        # Define table drop query
        drop_table_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        try:
            cursor.execute(drop_table_query)
            context.log.info(f"Table {table_name} dropped successfully.")
        except mc.Error as err:
            context.log.info(f"Error: {err}")
        finally:
            cursor.close()

    @staticmethod
    def add_constraint(connection, table_name, reference_tables,
                       foreign_key_columns, context):
        if len(reference_tables) != len(foreign_key_columns):
            raise ValueError("Length of 'reference_tables' and 'foreign_key_columns' must be equal.")
        cursor = connection.cursor()
        for i, reference_table in enumerate(reference_tables):
            foreign_key = foreign_key_columns[i]

            constraint_name = f"fk_{table_name}_{reference_table}"
            add_constraint_query = f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT {constraint_name}
                FOREIGN KEY ({foreign_key})
                REFERENCES {reference_table} (id);
            """
            cursor.execute(add_constraint_query)
            connection.commit()
            context.log.info(f"Added foreign key constraint '{constraint_name}'")
        cursor.close()

    @staticmethod
    def add_constraint_cross(connection, fact_table, reference_id,
                             reference_tables, foreign_key_columns, context):
        cursor = connection.cursor()
        if len(reference_tables) == len(foreign_key_columns):
            for i, reference_table in enumerate(reference_tables):
                foreign_key = foreign_key_columns[i]
                constraint_name = f"fk_{fact_table}_{reference_table}"
                add_constraint_query = f"""
                    ALTER TABLE {fact_table}
                    ADD CONSTRAINT {constraint_name}
                    FOREIGN KEY ({foreign_key})
                    REFERENCES {reference_table} ({reference_id});
                """
                cursor.execute(add_constraint_query)
                connection.commit()
                context.log.info(f"Added foreign key constraint '{constraint_name}'")
            cursor.close()
        else:
            reference_table = reference_tables[0]
            for i, foreign_key in enumerate(foreign_key_columns):
                constraint_name = f"fk_{fact_table}_{reference_table}_{foreign_key}"
                add_constraint_query = f"""
                    ALTER TABLE {fact_table}
                    ADD CONSTRAINT {constraint_name}
                    FOREIGN KEY ({foreign_key})
                    REFERENCES {reference_table} ({reference_id});
                """
                cursor.execute(add_constraint_query)
                connection.commit()
                context.log.info(f"Added foreign key constraint '{constraint_name}'")
            cursor.close()

    @staticmethod
    def insert_data(connection, table, obj: pd.DataFrame, context):
        cursor = connection.cursor()
        try:
            # Replace NaT with None in DataFrame
            obj = obj.replace({pd.NaT: None})
            context.log.info(f"Replace NaT with None successfully")
            # Replace Na with None in DataFrame
            obj = obj.replace({pd.NA: None})
            context.log.info(f"Replace Na with None successfully")
            # Insert data into MySQL table
            for _, row in obj.iterrows():
                values = ', '.join([f'"{value}"' for value in row])
                insert_query = f"INSERT INTO {table} VALUES ({values});"
                cursor.execute(insert_query)
            # Commit the changes
            connection.commit()
            context.log.info('Load data into MySQL finished !')
        except mc.Error as e:
            context.log.info(f"Error: {e}")
        finally:
            cursor.close()

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        table_name = context.asset_key.path[-1].lower()
        columns_type = context.output_metadata.get('columns')[0]
        primary_keys = context.output_metadata.get('primary_keys')[0]
        foreign_keys = context.output_metadata.get('foreign_keys')[0]
        skip_status = context.output_metadata.get('skip_insertion_status')[0]
        additional_foreign_keys = context.output_metadata.get('additional_foreign_keys')[0]

        try:
            reference_tables = context.output_metadata.get('reference_tables')[0]
            context.log.info("Reference Tables")
            context.log.info(reference_tables)
        except Exception as e:
            reference_tables = None
            context.log.info(f"Table {table_name} has no Reference Tables")
            context.log.info(f"Reference Tables : {reference_tables} ({e})")

        try:
            additional_reference_tables = context.output_metadata.get('additional_reference_tables')[0]
            context.log.info("Additional Reference Tables")
            context.log.info(additional_reference_tables)
            context.log.info("Additional Foreign Keys")
            context.log.info(additional_foreign_keys)
        except Exception as e:
            additional_reference_tables = None
            context.log.info(f"Additional Reference Tables : {additional_reference_tables} ({e})")

        try:
            reference_schema = context.output_metadata.get('reference_schema')[0]
            context.log.info("Reference Schema")
            context.log.info(reference_schema)
        except Exception as e:
            reference_schema = None
            context.log.info(f"Reference Schema : {reference_schema} ({e})")

        try:
            reference_id = context.output_metadata.get('reference_id')[0]
            context.log.info("Reference Id Column")
            context.log.info(reference_id)
        except Exception as e:
            reference_id = None
            context.log.info(f"Reference Id Column : {reference_id} ({e})")

        context.log.info(obj)
        context.log.info("Columns Name")
        context.log.info(columns_type)

        context.log.info("Primary Keys")
        context.log.info(primary_keys)

        context.log.info("Foreign Keys")
        context.log.info(foreign_keys)

        context.log.info("Target table: ")
        context.log.info(table_name)
        columns = obj.columns.tolist()

        context.log.info("Columns name :")
        context.log.info(columns)

        connection = self.create_connection()
        context.log.info("Create connection successfully ")

        # Drop database if exists
        self.drop_table_if_not_exists(connection, table_name, context)

        # Create table
        self.create_table_if_not_exists(connection, table_name, columns_type, context, primary_keys)

        if reference_schema is not None and reference_id is not None:
            self.add_constraint_cross(connection, table_name, reference_id,
                                      reference_tables, foreign_keys, context)
        elif reference_tables is not None:
            self.add_constraint(connection, table_name, reference_tables,
                                foreign_keys, context)

        if additional_reference_tables is not None:
            self.add_constraint(connection, table_name, additional_reference_tables,
                                additional_foreign_keys, context)

        # Insert data into MySQL table
        if skip_status == 0:
            self.insert_data(connection, table_name, obj, context)

        connection.close()
        context.log.info("Process finished")

    def load_input(self, context: InputContext) -> None:
        context.add_input_metadata(metadata=context.upstream_output.output_metadata)
