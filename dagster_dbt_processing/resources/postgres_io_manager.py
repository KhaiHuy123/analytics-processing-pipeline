
import pandas as pd
from dagster import IOManager, OutputContext, InputContext, Output
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2 import sql
from datetime import datetime

current_day = datetime.now()
day_month_year = current_day.strftime("%d%m%y")


class PostgresSQL_IOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def get_conn(self):
        conn = psycopg2.connect(
            host=self._config["host"],
            port=self._config["port"],
            database=self._config["database"],
            user=self._config["user"],
            password=self._config["password"])
        try:
            return conn
        except Exception as e:
            raise ValueError(f'ERROR: {e}')

    def drop_table_if_exists(self, schema_name, table_name):
        with self.get_conn() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                drop_table_query = sql.SQL(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE")
                cursor.execute(drop_table_query)

    def create_table_if_not_exists(self, schema_name, table_name,
                                   col_types, context, primary_keys=None):
        with self.get_conn() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                # Check if table exists
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    );
                    """,
                    (schema_name, table_name)
                )
                table_exists = cursor.fetchone()[0]
                if table_exists:
                    context.log.info(f"Table {schema_name}.{table_name} has already exists. Skipping table creation.")
                    return
                # Convert metadata to SQL column definitions
                column_definitions = [
                    sql.SQL("{} {}").format(
                        sql.Identifier(column_name),
                        sql.SQL(column_type)
                    )
                    for column_dict in col_types
                    for column_name, column_type in column_dict.items()
                ]
                # Add primary key information to the column definitions
                if primary_keys:
                    column_definitions.append(
                        sql.SQL("PRIMARY KEY ({})").format(
                            sql.SQL(", ").join(map(sql.Identifier, primary_keys))
                        )
                    )
                create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(column_definitions)
                )
                cursor.execute(create_table_query)

                # Ensure TIMESTAMP columns allow NULL values
                for column_dict in col_types:
                    for column_name, column_type in column_dict.items():
                        if 'timestamp' in column_type.lower():
                            alter_column_query = sql.SQL("""
                                ALTER TABLE {}.{}
                                ALTER COLUMN {} DROP NOT NULL;
                            """).format(
                                sql.Identifier(schema_name),
                                sql.Identifier(table_name),
                                sql.Identifier(column_name)
                            )
                            cursor.execute(alter_column_query)
                            context.log.info(f'Alter column {column_name} to allow NULL values')
                context.log.info(f'Create table {schema_name}.{table_name} successfully')

    def insert_data(self, schema_name, table_name,
                    obj: pd.DataFrame, columns, context):
        with self.get_conn() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                # Check if table has existing data
                cursor.execute(
                    sql.SQL("""
                        SELECT COUNT(*)
                        FROM {}.{}
                    """).format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name)
                    )
                )
                existing_data_count = cursor.fetchone()[0]
                if existing_data_count > 0:
                    context.log.info(f"Table {schema_name}.{table_name} already has data. Skipping data insertion.")
                    return
                insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({});").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join([sql.SQL("%s")] * len(columns))
                )
                # Replace NaT with None in DataFrame
                obj = obj.replace({pd.NaT: None})
                context.log.info(f"Replace NaT with None successfully")

                # Replace NA with None in DataFrame
                obj = obj.replace({pd.NA: None})
                context.log.info(f"Replace NA with None successfully")

                data_values = [tuple(row) for row in obj.itertuples(index=False, name=None)]
                cursor.executemany(insert_query, data_values)
        context.log.info(f"Inserted data into PostgresSQL {schema_name}.{table_name} successfully")

    def add_constraint(self, schema_name, fact_table,
                       list_reference_tables, list_foreign_key, context):
        if len(list_reference_tables) != len(list_foreign_key):
            raise ValueError("Length of 'reference_tables' and 'foreign_key_columns' must be equal.")

        with self.get_conn() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                for i, reference_table in enumerate(list_reference_tables):
                    foreign_key = list_foreign_key[i]

                    # Ensure foreign_key columns allow NULL values
                    alter_column_query = f"""
                        ALTER TABLE {schema_name}.{fact_table}
                        ALTER COLUMN {foreign_key} DROP NOT NULL;
                    """
                    cursor.execute(alter_column_query)
                    context.log.info(f"Alter column {foreign_key} to allow NULL values")

                    constraint_name = f"fk_{fact_table}_{reference_table}"
                    add_constraint_query = sql.SQL(f"""
                        ALTER TABLE {schema_name}.{fact_table}
                        ADD CONSTRAINT {constraint_name}
                        FOREIGN KEY ({foreign_key})
                        REFERENCES {schema_name}.{reference_table} (id);
                        """)
                    cursor.execute(add_constraint_query)
                    context.log.info(f"Added foreign key constraint '{constraint_name}'")

    def add_constraint_cross(self, schema_name, schema_ref, fact_table, reference_id,
                             list_reference_tables, list_foreign_key, context):
        with self.get_conn() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                if len(list_reference_tables) == len(list_foreign_key):
                    for i, reference_table in enumerate(list_reference_tables):
                        foreign_key = list_foreign_key[i]

                        # Ensure foreign_key columns allow NULL values
                        alter_column_query = f"""
                            ALTER TABLE {schema_name}.{fact_table}
                            ALTER COLUMN {foreign_key} DROP NOT NULL;
                        """
                        cursor.execute(alter_column_query)
                        context.log.info(f"Alter column {foreign_key} to allow NULL values")

                        constraint_name = f"fk_{fact_table}_{reference_table}"
                        add_constraint_query = sql.SQL(f"""
                            ALTER TABLE {schema_name}.{fact_table}
                            ADD CONSTRAINT {constraint_name}
                            FOREIGN KEY ({foreign_key})
                            REFERENCES {schema_ref}.{reference_table} ({reference_id});
                        """)
                        cursor.execute(add_constraint_query)
                        context.log.info(f"Added foreign key constraint '{constraint_name}'")
                else:
                    reference_table = list_reference_tables[0]
                    for i, foreign_key in enumerate(list_foreign_key):

                        # Ensure foreign_key columns allow NULL values
                        alter_column_query = f"""
                            ALTER TABLE {schema_name}.{fact_table}
                            ALTER COLUMN {foreign_key} DROP NOT NULL;
                        """
                        cursor.execute(alter_column_query)
                        context.log.info(f"Alter column {foreign_key} to allow NULL values")

                        constraint_name = f"fk_{fact_table}_{reference_table}_{foreign_key}"
                        add_constraint_query = sql.SQL(f"""
                            ALTER TABLE {schema_name}.{fact_table}
                            ADD CONSTRAINT {constraint_name}
                            FOREIGN KEY ({foreign_key})
                            REFERENCES {schema_ref}.{reference_table} ({reference_id});
                        """)
                        cursor.execute(add_constraint_query)
                        context.log.info(f"Added foreign key constraint '{constraint_name}'")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
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

        context.log.info("Insert Data into PostgresSQL")
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

        # Drop database if exists
        self.drop_table_if_exists(self._config["schema"], table_name)

        # Create table
        self.create_table_if_not_exists(self._config["schema"], table_name, columns_type, context, primary_keys)

        if reference_schema is not None and reference_id is not None:
            self.add_constraint_cross(self._config["schema"], reference_schema, table_name, reference_id,
                                      reference_tables, foreign_keys, context)
        elif reference_tables is not None:
            self.add_constraint(self._config["schema"], table_name, reference_tables, foreign_keys, context)

        if additional_reference_tables is not None:
            self.add_constraint(self._config["schema"], table_name, additional_reference_tables,
                                additional_foreign_keys, context)

        # Insert data into PostgresSQL table
        if skip_status == 0:
            self.insert_data(self._config["schema"], table_name, obj, columns, context)

    def load_input(self, context: InputContext) -> Output[pd.DataFrame]:
        table_name = context.name
        # Load data from PostgresSQL table directly into a DataFrame
        with self.get_conn() as conn:
            select_query = f"SELECT * FROM {self._config['schema']}.{table_name};"
            with conn.cursor() as cursor:
                cursor.execute(select_query)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        return Output(value=df)
