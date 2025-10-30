from airflow import DAG
from airflow.decorators import task
import requests
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
import pendulum

DB_CONFIG = {
    'user': 'postgres',
    'pass' : '87214249',
    'host' : '192.168.101.4',
    'db'   : 'postgres',
    'port' : '5432',
    'schema': 'sch_transferegov',
    'table': 'empenho'
}

with DAG(
    'dag_transferegov_empenho',
    start_date=pendulum.now().subtract(days=1),
    schedule='@daily',
    catchup=False,
    tags=['transferegov', 'empenho']
) as dag:

    @task
    def extract_api_data(endpoint="empenho", limit=1000):
        url = f"https://api.transferegov.gestao.gov.br/fundoafundo/{endpoint}"
        headers = {"Accept": "application/json"}
        todos_dados = []
        offset = 0

        while True:
            params = {"offset": offset, "limit": limit}
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            dados = response.json()
            if not dados:
                break
            todos_dados.extend(dados)
            offset += limit

        return todos_dados 

    @task
    def transform_data(dados_empenho):
        cfg = DB_CONFIG
        engine = create_engine(f"postgresql+psycopg2://{cfg['user']}:{cfg['pass']}@{cfg['host']}:{cfg['port']}/{cfg['db']}")
         

        plano_acao = pd.read_sql("SELECT id_programa, id_plano_acao FROM sch_transferegov.plano_acao", engine)
        df = pd.DataFrame(dados_empenho)
        
        df_final = df.merge(plano_acao, on="id_plano_acao", how="inner")       
        df_final['data_atualizacao'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        return df_final.to_json(orient='records')

    @task
    def load_data(df_json):
        cfg = DB_CONFIG
        engine = create_engine(f"postgresql+psycopg2://{cfg['user']}:{cfg['pass']}@{cfg['host']}:{cfg['port']}/{cfg['db']}")
        # Corrigido: usar StringIO para ler o JSON
        df_final = pd.read_json(StringIO(df_json), orient='records')

        df_final.to_sql(
            name=cfg['table'],
            con=engine,
            schema=cfg['schema'],
            if_exists='replace',
            index=False,
            method='multi'
        )
        
    #encadeamento
    dados = extract_api_data() 
    df_transformado = transform_data(dados) 
    load_data(df_transformado)