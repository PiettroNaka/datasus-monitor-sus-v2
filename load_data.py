import pandas as pd
import sqlite3
import os

def load_formatted_csv_to_db(csv_path, table_name, db_path):
    if not os.path.exists(csv_path):
        print(f"Arquivo {csv_path} não encontrado.")
        return
    
    try:
        # Carregar o CSV formatado
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
        
        # Identificar colunas que não são de metadados (metadados: Municipio, ANO, MES)
        meta_cols = ['Municipio', 'ANO', 'MES']
        # Se o CSV tiver outros nomes para município, pegamos a primeira coluna
        if df.columns[0] not in meta_cols:
            meta_cols[0] = df.columns[0]
            
        data_cols = [c for c in df.columns if c not in meta_cols]
        
        # Forçar conversão numérica para todas as colunas de dados
        for col in data_cols:
            # Substituir hífens, espaços e converter para float, preenchendo erros com 0
            df[col] = pd.to_numeric(df[col].astype(str).replace('-', '0').replace('', '0'), errors='coerce').fillna(0)
            
        # Conectar ao banco
        conn = sqlite3.connect(db_path)
        
        # Salvar no banco
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        print(f"Tabela {table_name} atualizada com {len(df)} registros e {len(df.columns)} colunas numéricas.")
    except Exception as e:
        print(f"Erro ao carregar {table_name}: {e}")

if __name__ == "__main__":
    db_path = "/home/ubuntu/datasus-v2/datasus.db"
    load_formatted_csv_to_db("/home/ubuntu/datasus-v2/sih_formatted.csv", "sih_data", db_path)
    load_formatted_csv_to_db("/home/ubuntu/datasus-v2/sia_formatted.csv", "sia_data", db_path)
