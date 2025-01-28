import os
import time
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from dune_client.client import DuneClient

def load_env_variables():
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path)

    return {
        'USER': os.environ.get('SNOWFLAKE_USER'),
        'PASSWORD': os.environ.get('SNOWFLAKE_PASSWORD'),
        'ACCOUNT': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'WAREHOUSE': os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'DATABASE': os.environ.get('SNOWFLAKE_DATABASE'),
        'SCHEMA': os.environ.get('SNOWFLAKE_SCHEMA')
    }

def start_snowflake_connection(credentials):
    return snowflake.connector.connect(
        user=credentials['USER'],
        password=credentials['PASSWORD'],
        account=credentials['ACCOUNT'],
        warehouse=credentials['WAREHOUSE'],
        database=credentials['DATABASE'],
        schema=credentials['SCHEMA']
    )

def close_snowflake_connection(conn):
    conn.close()

def execute_query(conn, query, title):
    cursor = conn.cursor()
    print(f"Executing query: {title} ...")
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()
    return df

def get_dune_client():
    return DuneClient.from_env()

# Function to delete a table from Dune
def delete_existing_table(dune, namespace, table_name):
    try:
        dune.delete_table(namespace=namespace, table_name=table_name)
        print(f"Successfully deleted the table: {table_name}")
    except Exception as e:
        print(f"Failed to delete the table: {table_name}. Error: {e}")


def save_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"Query executed successfully. Data saved to {output_path}.")

def main():
    credentials = load_env_variables()
    dune = get_dune_client()
    start_time = time.time()

    print("Connecting to Snowflake...")
    conn = start_snowflake_connection(credentials)
    print("Snowflake connection established!")
    print(f"SNOWFLAKE_USER: {credentials['USER']}")
    print(f"SNOWFLAKE_ACCOUNT: {credentials['ACCOUNT']}")
    print(f"SNOWFLAKE_WAREHOUSE: {credentials['WAREHOUSE']}")
    print(f"SNOWFLAKE_DATABASE: {credentials['DATABASE']}")
    print(f"SNOWFLAKE_SCHEMA: {credentials['SCHEMA']}")

    query_general_metrics = """
        with base as (
            SELECT DATE(txns.block_timestamp) AS day
                
                , count(distinct txns.hash) as tx_count    
                , count(distinct txns.FROM_ADDRESS) as active_addr
            
                
                , count(distinct contracts.address) as num_contracts
                , count(distinct contracts.deployer) as num_deployers
            FROM transactions AS txns
            INNER JOIN blocks ON blocks.number = txns.block_number
            LEFT JOIN contracts on contracts.block_number = txns.block_number
            WHERE date_trunc('day', txns.BLOCK_TIMESTAMP) < current_date 
            GROUP BY ALL
        )

        SELECT day

            , tx_count
            , AVG(tx_count) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_tx_last_7_days
            , ((AVG(tx_count) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) - 
                (SUM(tx_count) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0))
                / NULLIF((SUM(tx_count) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0), 0)
            ) AS wow_tx_growth_rate

            , active_addr
            , AVG(active_addr) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_eoa_last_7_days
            , ((AVG(active_addr) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) - 
                (SUM(active_addr) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0))
                / NULLIF((SUM(active_addr) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0), 0)
            ) AS wow_eoa_growth_rate

            , num_contracts
            , AVG(num_contracts) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_contracts_last_7_days
            , ((AVG(num_contracts) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) - 
                (SUM(num_contracts) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0))
                / NULLIF((SUM(num_contracts) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0), 0)
            ) AS wow_contracts_growth_rate

            , num_deployers
            , AVG(num_deployers) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_deployers_last_7_days
            , (SUM(num_deployers) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0) AS avg_deployers_8_to_14_days
            , ((AVG(num_deployers) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) - 
                (SUM(num_deployers) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0))
                / NULLIF((SUM(num_deployers) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) / 7.0), 0)
            ) AS wow_deployers_growth_rate
        FROM base
        order by day desc
    """
    
    query_new_and_returning_eoas = """
        WITH first_appearance AS (
            SELECT MIN(block_timestamp) AS first_block_time
            , from_address AS user
            FROM transactions
            WHERE from_address NOT IN (SELECT address FROM contracts)
            AND date_trunc('day', BLOCK_TIMESTAMP) < current_date 
            GROUP BY 2
            )

        , new_users AS (
            SELECT date_trunc('day', first_block_time) AS time
            , CAST(COUNT(user) AS double) AS new_users
            FROM first_appearance
            GROUP BY 1
            )

        , total_data AS (
            SELECT date_trunc('day', block_timestamp) AS time
            , CAST(COUNT(distinct from_address) AS double) AS total_users
            FROM transactions
            WHERE from_address NOT IN (SELECT address FROM contracts)
            AND date_trunc('day', BLOCK_TIMESTAMP) < current_date 
            GROUP BY 1
            )

        SELECT td.time as day
        , td.total_users-nu.new_users AS returning_addresses
        , nu.new_users as new_addresses
        , AVG(td.total_users) OVER(ORDER BY td.time ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS users_30d_moving_average
        FROM total_data td
        LEFT JOIN new_users nu ON td.time=nu.time
    """

    query_new_and_returning_deployers = """
        WITH first_appearance AS (
            SELECT MIN(block_timestamp) AS first_block_time
            , deployer
            FROM contracts
            where date_trunc('day', BLOCK_TIMESTAMP) < current_date 
            GROUP BY 2
            )

        , new_deployers AS (
            SELECT date_trunc('day', first_block_time) AS time
            , CAST(COUNT(deployer) AS double) AS new_deployers
            FROM first_appearance
            GROUP BY 1
            )

        , total_data AS (
            SELECT date_trunc('day', block_timestamp) AS time
            , CAST(COUNT(distinct deployer) AS double) AS total_deployers
            FROM contracts
            where date_trunc('day', BLOCK_TIMESTAMP) < current_date 
            GROUP BY 1
            )

        SELECT td.time as day
        , td.total_deployers-nu.new_deployers AS returning_deployers
        , nu.new_deployers
        , AVG(td.total_deployers) OVER(ORDER BY td.time ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS deployers_30d_moving_average
        FROM total_data td
        LEFT JOIN new_deployers nu ON td.time=nu.time
    """

    query_gas_metrics = """
        with base as (
            SELECT DATE(txns.block_timestamp) AS day
                , SUM(
                    receipt_effective_gas_price * receipt_gas_used / POWER(10, 18)
                ) AS l2_fees
                , SUM(
                    (
                    receipt_effective_gas_price - COALESCE(blocks.base_fee_per_gas, 0)
                    ) * receipt_gas_used / POWER(10, 18)
                ) AS l2_priority_fees
                , SUM(
                    COALESCE(blocks.base_fee_per_gas, 0) * receipt_gas_used / POWER(10, 18)
                ) AS l2_base_fees
                , SUM(COALESCE(receipt_l1_fee, '0') / POWER(10, 18)) AS l1_fees

                , median(receipt_effective_gas_price / 1e9) as median_gas_price_gwei
            FROM transactions AS txns
            INNER JOIN blocks ON blocks.number = txns.block_number
            LEFT JOIN contracts on contracts.block_number = txns.block_number
            WHERE date_trunc('day', txns.BLOCK_TIMESTAMP) < current_date 
                and txns.receipt_effective_gas_price > 0 -- filter out system transactions if we are doing revenue
            GROUP BY ALL
        )

        SELECT day
            , l2_priority_fees
            , l2_base_fees
            , l1_fees
            , median_gas_price_gwei
        FROM base
        order by day desc
    """

    query_gas_guzzlers = """
    with data AS (
        SELECT coalesce(txs.to_address, txs.RECEIPT_CONTRACT_ADDRESS) AS address
        , SUM(txs.receipt_effective_gas_price * txs.receipt_gas_used / POWER(10, 18)) AS gas_used -- l2_fees
        , COUNT(distinct txs.from_address) AS address_that_interacted
        , CAST(COUNT(*) AS double) AS tx_count
        FROM transactions txs
        WHERE BLOCK_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP)
            and txs.receipt_effective_gas_price > 0 -- filter out system transactions if we are doing revenue
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10000
    )

    , total AS (
        SELECT SUM(txs.receipt_effective_gas_price * txs.receipt_gas_used / POWER(10, 18)) AS gas_used
        , CAST(COUNT(*) AS double) AS tx_count
        FROM transactions txs
        WHERE BLOCK_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP)
            and txs.receipt_effective_gas_price > 0 -- filter out system transactions if we are doing revenue
        )

    SELECT ROW_NUMBER() OVER (ORDER BY d.gas_used DESC) AS ranking
        , address
        , d.gas_used
        , d.address_that_interacted
        , d.gas_used/(SELECT SUM(gas_used) FROM total) AS percentage_of_gas_used
        , d.tx_count
        , d.tx_count/(SELECT SUM(tx_count) FROM total) AS percentage_of_txs
    FROM data d
    """
    
    query_gas_spenders = """
        with data AS (
            SELECT txs.from_address AS address
            , SUM(txs.receipt_effective_gas_price * txs.receipt_gas_used / POWER(10, 18)) AS gas_used -- l2_fees
            , COUNT(distinct txs.from_address) AS address_that_interacted
            , CAST(COUNT(*) AS double) AS tx_count
            FROM transactions txs
            WHERE BLOCK_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP)
                and txs.receipt_effective_gas_price > 0 -- filter out system transactions if we are doing revenue
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 10000
        )

        , total AS (
            SELECT SUM(txs.receipt_effective_gas_price * txs.receipt_gas_used / POWER(10, 18)) AS gas_used
            , CAST(COUNT(*) AS double) AS tx_count
            FROM transactions txs
            WHERE BLOCK_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP)
                and txs.receipt_effective_gas_price > 0 -- filter out system transactions if we are doing revenue
            )

        SELECT ROW_NUMBER() OVER (ORDER BY d.gas_used DESC) AS ranking
            , address
            , d.gas_used
            , d.address_that_interacted
            , d.gas_used/(SELECT SUM(gas_used) FROM total) AS percentage_of_gas_used
            , d.tx_count
            , d.tx_count/(SELECT SUM(tx_count) FROM total) AS percentage_of_txs
        FROM data d

    """
    
    df_general_metrics = execute_query(conn, query_general_metrics, 'General Metrics')
    df_new_and_returning_eoas = execute_query(conn, query_new_and_returning_eoas, 'New and Returning EOAs')
    df_new_and_returning_deployers = execute_query(conn, query_new_and_returning_deployers, 'New and Returning Deployers')
    df_gas_metrics = execute_query(conn, query_gas_metrics, 'Gas Metrics')
    df_gas_guzzlers = execute_query(conn, query_gas_guzzlers, 'Gas Guzzlers')
    df_gas_spenders = execute_query(conn, query_gas_spenders, 'Gas Spenders')
    # save_to_csv(df_general_metrics, 'general_metrics.csv')

    # Delete the existing table before uploading new data
    delete_existing_table(dune, "uniswap_fnd", "dataset_unichain_sepolia_general_metrics")
    delete_existing_table(dune, "uniswap_fnd", "dataset_unichain_sepolia_new_and_returning_eoas")
    delete_existing_table(dune, "uniswap_fnd", "dataset_unichain_sepolia_new_and_returning_deployers")
    delete_existing_table(dune, "uniswap_fnd", "dataset_unichain_sepolia_gas_metrics")
    delete_existing_table(dune, "uniswap_fnd", "dataset_unichain_sepolia_gas_guzzlers")
    delete_existing_table(dune, "uniswap_fnd", "dataset_unichain_sepolia_gas_spenders")

    # Upload to Dune
    upload_success = dune.upload_csv(
        table_name="unichain_sepolia_general_metrics",
        data=df_general_metrics.to_csv(index=False),
        is_private=True
    )
    print("Uploaded General Metrics successfully:", upload_success)
    
    upload_success = dune.upload_csv(
        table_name="unichain_sepolia_new_and_returning_eoas",
        data=df_new_and_returning_eoas.to_csv(index=False),
        is_private=True
    )
    print("Uploaded new and returning EOAs successfully:", upload_success)

    upload_success = dune.upload_csv(
        table_name="unichain_sepolia_new_and_returning_deployers",
        data=df_new_and_returning_deployers.to_csv(index=False),
        is_private=True
    )
    print("Uploaded new and returning deployers successfully:", upload_success)

    upload_success = dune.upload_csv(
        table_name="unichain_sepolia_gas_metrics",
        data=df_gas_metrics.to_csv(index=False),
        is_private=True
    )
    print("Uploaded Gas Metrics successfully:", upload_success)

    upload_success = dune.upload_csv(
        table_name="unichain_sepolia_gas_guzzlers",
        data=df_gas_guzzlers.to_csv(index=False),
        is_private=True
    )
    print("Uploaded Gas Guzzlers successfully:", upload_success)

    upload_success = dune.upload_csv(
        table_name="unichain_sepolia_gas_spenders",
        data=df_gas_spenders.to_csv(index=False),
        is_private=True
    )
    print("Uploaded Gas Spenders successfully:", upload_success)
    
    
    close_snowflake_connection(conn)

    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    print(f"Process completed in {int(minutes)} minutes and {int(seconds)} seconds.")

if __name__ == "__main__":
    main()
