import requests
import pandas as pd

# funcao extrair da api (PE)
def extract_api_data(endpoint="plano_acao", limit=1000):
    url = f"https://api.transferegov.gestao.gov.br/fundoafundo/{endpoint}"
    headers = {"Accept": "application/json"}
    todos_dados = []
    offset = 0

    # Lista de CNPJs desejados
    cnpjs_desejados = {
        "10571982000125",
        "02960040000100",
        "06290858000114",
        "08693255000199"
    }

    while True:
        params = {"offset": offset, "limit": limit}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        dados = response.json()
        if not dados:
            break

        # Filtra os dados com base nos CNPJs
        filtrados = [
            item for item in dados
            if str(item.get("cnpj_ente_recebedor_plano_acao")) in cnpjs_desejados
        ]

        todos_dados.extend(filtrados)
        offset += limit

    return todos_dados

# funcao transformar dados em df
def transform_data(dados_plano_acao):
    
    df = pd.DataFrame(dados_plano_acao)
    df['data_atualizacao'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    return df.to_json(orient='records')

#encadeamento
dados = extract_api_data() 
df_transformado = transform_data(dados)