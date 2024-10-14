from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3

# Argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['ana.rodrigues@indicium.tech'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Função para Task 1 - Extrair dados da tabela 'Order' e salvar como CSV
def extract_orders():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT * FROM 'Order'"
    df_orders = pd.read_sql(query, conn)
    df_orders.to_csv('output_orders.csv', index=False)
    conn.close()

# Função para Task 2 - Fazer JOIN com OrderDetail, calcular a soma de Quantity para o RJ e salvar em count.txt
def join_and_calculate():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    df_orders = pd.read_csv('output_orders.csv')

    # Verificar as colunas de df_orders
    print("Colunas de df_orders:", df_orders.columns)

    query = "SELECT * FROM OrderDetail"
    df_order_details = pd.read_sql(query, conn)

    # Verificar as colunas de df_order_details
    print("Colunas de df_order_details:", df_order_details.columns)
    
    # Fazer o JOIN entre 'Order' e 'OrderDetail'
    df_joined = pd.merge(df_order_details, df_orders, left_on='OrderId', right_on='Id', how='inner')
    
    # Filtrar para 'ShipCity' Rio de Janeiro e somar a quantidade (Quantity)
    rj_quantity_sum = df_joined[df_joined['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()
    
    # Salvar o resultado em count.txt
    with open('count.txt', 'w') as f:
        f.write(str(rj_quantity_sum))
    
    conn.close()

# Função para Task 3 - Exportar arquivo final de output
def export_final_output():
    with open('final_output.txt', 'w') as f:
        f.write('Texto codificado final gerado automaticamente')

# Definição do DAG
with DAG(
    dag_id='etl_northwind',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL do banco de dados Northwind',
) as dag:

    task1 = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders
    )

    task2 = PythonOperator(
        task_id='join_and_calculate',
        python_callable=join_and_calculate
    )

    export_final_output_task = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_output
    )

    # Definir a sequência de execução
    task1 >> task2 >> export_final_output_task
