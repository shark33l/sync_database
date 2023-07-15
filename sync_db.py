import utils
from mysql_handler import SyncMySQLConnector, DestinationMySQLConnector
import pandas as pd

logger = utils.create_logger(__name__)
main_config = utils.get_config("main_config.json")

# database configs
database_configs = main_config.get("database_config")

# sync configs
sync_configs = main_config.get("sync_configs")

if __name__ == '__main__':

    '''
    --- Source DB Table ---
    1. Get Configs from main_config.json file
    2. Create connection using the MySQLConnector class
    3. Query required table
    '''
    # Create Source DB connection
    source_configs = database_configs.get("source")
    source_conn = SyncMySQLConnector(
        host=source_configs.get("host"),
        port=source_configs.get("port"),
        user=source_configs.get("user"),
        password=source_configs.get("password"),
        database=source_configs.get("database"),
        table=source_configs.get("table"),
        join_key=source_configs.get("join_key")
    )
    logger.info(f"Connected to Source DB - {source_conn.database}")

    # Load data from Source DB
    source_table_results = source_conn.query_table()
    logger.info(f"Queried {source_table_results.shape[0]} rows and {source_table_results.shape[1]} columns from {source_conn.database}.{source_conn.table}")

    '''
        --- Destination DB Table ---
        1. Get Configs from main_config.json file
        2. Create connection using the MySQLConnector class
        3. Query required table
        4. Process and Transform Data if required
    '''
    dest_configs = database_configs.get("destination")
    dest_conn = DestinationMySQLConnector(
        host=dest_configs.get("host"),
        port=dest_configs.get("port"),
        user=dest_configs.get("user"),
        password=dest_configs.get("password"),
        database=dest_configs.get("database"),
        table=dest_configs.get("table"),
        join_key=dest_configs.get("join_key"),
        primary_key=dest_configs.get("primary_key")
    )
    logger.info(f"Connected to Source DB - {dest_conn.database}")

    # Load data from Source DB
    dest_table_results = dest_conn.query_table()
    logger.info(f"Queried {dest_table_results.shape[0]} rows and {dest_table_results.shape[1]} columns from {dest_conn.database}.{dest_conn.table}")


    '''
    Process and reformat data
    '''
    logger.info(f"Initiating processing/transforming of data from {source_conn.database}.{source_conn.table} and {dest_conn.database}.{dest_conn.table} to identify data that needs to be synced.")
    # Convert byte array to string
    dest_table_results.dhcp_identifier = dest_table_results.dhcp_identifier.map(lambda mac_address: utils.convert_int2mac(mac_address))

    # Convert int IP Address to string like IP Address
    dest_table_results.ipv4_address = dest_table_results.ipv4_address.map(lambda ip_address: utils.convert_int2ip(int(ip_address)))

    source_dest_column_mappings = sync_configs.get("column_mappings")

    '''
        identify data that needs to be added/updated/removed to the Destination DB Table
    '''
    # Join both dataframes get what is not available on destination dataframe
    joined_df = pd.merge(
        source_table_results, dest_table_results,
        how="outer",
        left_on=source_conn.join_key,
        right_on=dest_conn.join_key,
        copy=True
    )

    '''
        --- INSERT ---
        New Data that needs to be Inserted
    '''
    # New data that needs to be inserted
    data_to_add_df = joined_df.query(f'{dest_conn.join_key}.isnull()', inplace=False)

    # Check if there's data to be added, if so insert to destination
    if data_to_add_df.shape[0] > 0:

        # Populate values that needs to inserted from the source
        for key, value in source_dest_column_mappings.items():
            if value == "ipv4_address":
                data_to_add_df.loc[:, value] = data_to_add_df.loc[:, key].map(
                    lambda ip_address: utils.convert_ip2int(ip_address))
            elif value == "dhcp_identifier":
                data_to_add_df.loc[:, value] = data_to_add_df.loc[:, key].map(
                    lambda mac_address: utils.convert_mac2int(mac_address))
            else:
                data_to_add_df.loc[:, value] = data_to_add_df.loc[:, key]

        # Populate other constant values that's required.
        # Add constant values
        for key, value in sync_configs.get("destination")["custom_constant_mappings"].items():
            data_to_add_df.loc[:, key] = value

        # Drop Columns that's not related to the Destination DB Table
        data_to_add_df = data_to_add_df.drop(source_table_results.columns.to_list(), axis=1, inplace=False)

        try:
            logger.info(f"INSERT | Inserting latest data to {dest_conn.database}.{dest_conn.table}")
            logger.info(f"INSERT | Identified {data_to_add_df.shape[0]} rows to be added.")
            data_to_add_df.to_sql(
                name=dest_conn.table,
                con=dest_conn.conn_engine,
                if_exists="append",
                index=False
            )
            logger.info(f"INSERT | Successfully added {data_to_add_df.shape[0]} rows to table - {dest_conn.database}.{dest_conn.table}")
        except Exception as exc:
            logger.exception(f"INSERT | Error when Inserting to the Destination table - {dest_conn.database}.{dest_conn.table}")
    else:
        logger.info("INSERT | No data was found to Insert")

    '''
        --- UPDATE ---
        Identifying and updating existing data which are updated on source DB table
    '''
    # Data that needs to be updated
    # Create query to identify data that's required to be updated
    query_params = ""
    for index, (key, value) in enumerate(source_dest_column_mappings.items()):
        if index != 0:
            query_params = f"{query_params} & "
        query_params = query_params + f"{key} == {value}"

    data_to_be_updated_df = joined_df.query(
        f'{source_conn.join_key}.notnull() & {dest_conn.join_key}.notnull() & not ({query_params})',
        inplace=False
    )

    # Check if there's data to be updated, if so update in destination
    if data_to_be_updated_df.shape[0] > 0:

        # Populate values that needs to be updated from the source
        for key, value in source_dest_column_mappings.items():
            if value == "ipv4_address":
                data_to_be_updated_df.loc[:, value] = data_to_be_updated_df.loc[:, key].map(
                    lambda ip_address: utils.convert_ip2int(ip_address))
            elif value == "dhcp_identifier":
                data_to_add_df.loc[:, value] = data_to_add_df.loc[:, key].map(
                    lambda mac_address: utils.convert_mac2int(mac_address))
            else:
                data_to_be_updated_df.loc[:, value] = data_to_be_updated_df.loc[:, key]

        # Drop Columns that's not related to the Destination DB Table
        data_to_be_updated_df = data_to_be_updated_df.drop(source_table_results.columns.to_list(), axis=1, inplace=False)

        logger.info(f"UPDATE | Updating {data_to_be_updated_df.shape[0]} rows in {dest_conn.database}.{dest_conn.table}")
        # Update rows that requires to be updated
        for index, (row_index, row) in enumerate(data_to_be_updated_df.iterrows()):

            # Get key value (Used in WHERE clause)
            key_value = row[dest_conn.primary_key]

            # Get parameters and values that needs to be updated
            update_parameters = {}
            for key, value in source_dest_column_mappings.items():
                if value != "dhcp_identifier":
                    update_parameters[value] = row[value]

            # Update row
            try:
                dest_conn.update_row(update_parameters=update_parameters, key_name=dest_conn.primary_key, key_value=key_value)
                logger.info(f"UPDATE | Successfully updated {index+1} of {data_to_be_updated_df.shape[0]} row/s in {dest_conn.database}.{dest_conn.table}")
            except Exception as exc:
                logger.exception(f"UPDATE | Error while updating a row in {dest_conn.database}.{dest_conn.table}", update_parameters)
    else:
        logger.info("UPDATE | There was no data to be updated.")

    '''
        --- DELETE ---
        Delete data that no longer exists on source DB table
    '''
    # Data that needs to be removed
    data_to_remove_df = joined_df.query('_snipeit_mac_address_1.isnull()', inplace=False)

    # Check if there's data to be deleted, if so delete from destination
    if data_to_remove_df.shape[0] > 0:
        # Drop Columns that's not related to the Destination DB Table
        data_to_remove_df = data_to_remove_df.drop(source_table_results.columns.to_list(), axis=1, inplace=False)

        logger.info(f"DELETE | Deleting {data_to_remove_df.shape[0]} rows in {dest_conn.database}.{dest_conn.table}")
        # Delete rows that do not exist on source DB Table
        for index, (row_index, row) in enumerate(data_to_remove_df.iterrows()):

            # Get key value (Used in WHERE clause)
            key_value = row[dest_conn.primary_key]

            try:
                # Delete row
                dest_conn.delete_row(key_name=dest_conn.primary_key, key_value=key_value)
                logger.info(f"DELETE | Deleted {index+1}/{data_to_remove_df.shape[0]} in {dest_conn.database}.{dest_conn.table}")
                logger.info(row.to_dict())
            except Exception as exc:
                logger.exception(f"DELETE | Error when deleting a row in {dest_conn.database}.{dest_conn.table}")
                logger.error(row.to_dict())
    else:
        logger.info("DELETE | There was no data to be deleted.")



