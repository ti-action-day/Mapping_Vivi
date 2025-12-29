import os
import requests
import pandas as pd
import json
import base64
from dotenv import load_dotenv
from google.oauth2 import service_account
import pandas_gbq

# --- CONFIGURAÇÕES ---
load_dotenv()

class KommoAuthenticator:
    def __init__(self):
        self.subdomain = os.getenv("KOMMO_SUBDOMAIN")
        self.long_token = os.getenv("KOMMO_LONG_TOKEN")
    def get_headers(self):
        return {"Authorization": f"Bearer {self.long_token}", "Content-Type": "application/json"}

def obter_credenciais_gcp():
    b64_creds = os.getenv("GCP_CREDENTIALS_BASE64")
    padding = len(b64_creds) % 4
    if padding > 0: b64_creds += "=" * (4 - padding)
    creds_json = base64.b64decode(b64_creds).decode('utf-8')
    creds_dict = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(creds_dict), creds_dict.get("project_id")

def carregar_tabela(df, nome_tabela, creds, project_id, dataset):
    full_table = f"{dataset}.{nome_tabela}"
    print(f"Carregando {len(df)} linhas em {full_table}...")
    pandas_gbq.to_gbq(df, destination_table=full_table, project_id=project_id, credentials=creds, if_exists='replace')

def main():
    creds, project_id = obter_credenciais_gcp()
    dataset_id = os.getenv("GCP_DATASET")
    auth = KommoAuthenticator()
    base_url = f"https://{auth.subdomain}.kommo.com"

    # --- 1. EXTRAIR USUÁRIOS ---
    print("Baixando Usuários...")
    resp_users = requests.get(f"{base_url}/api/v4/users", headers=auth.get_headers())
    users_data = []
    if resp_users.status_code == 200:
        for u in resp_users.json()['_embedded']['users']:
            users_data.append({
                'user_id': str(u['id']),
                'nome_usuario': u['name'],
                'email': u['email']
            })
        df_users = pd.DataFrame(users_data)
        carregar_tabela(df_users, "kommo_vivi_dim_users", creds, project_id, dataset_id)

    # --- 2. EXTRAIR FUNIS E STATUS ---
    print("Baixando Funis e Status...")
    resp_pipelines = requests.get(f"{base_url}/api/v4/leads/pipelines", headers=auth.get_headers())
    status_data = []
    if resp_pipelines.status_code == 200:
        for pipe in resp_pipelines.json()['_embedded']['pipelines']:
            pipe_id = str(pipe['id'])
            pipe_name = pipe['name']
            
            # Adiciona os status desse funil
            for status in pipe['_embedded']['statuses']:
                status_data.append({
                    'status_id': str(status['id']),
                    'nome_status': status['name'],
                    'pipeline_id': pipe_id,
                    'nome_funil': pipe_name,
                    'cor': status.get('color')
                })
                
    df_status = pd.DataFrame(status_data)
    # Adiciona status padrão de sistema (Sucesso/Perda) se não vierem na API
    # As vezes o 142 e 143 não vêm na lista de pipelines
    if not df_status[df_status['status_id'] == '142'].shape[0]:
        extra = pd.DataFrame([
            {'status_id': '142', 'nome_status': 'Sucesso', 'pipeline_id': '0', 'nome_funil': 'Sistema', 'cor': '#CCFFCC'},
            {'status_id': '143', 'nome_status': 'Perdido', 'pipeline_id': '0', 'nome_funil': 'Sistema', 'cor': '#FFCCCC'}
        ])
        df_status = pd.concat([df_status, extra], ignore_index=True)

    carregar_tabela(df_status, "kommo_vivi_dim_statuses", creds, project_id, dataset_id)
    print("Mapeamento concluído!")

if __name__ == "__main__":
    main()
