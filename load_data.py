import pandas as pd
import sqlite3
import os

def load_formatted_csv_to_db(csv_path, table_name, db_path):
    if not os.path.exists(csv_path):
        print(f"Arquivo {csv_path} não encontrado.")
        return
    
    try:
        # Carregar o CSV formatado pelo novo get_data.py
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
        
        # Conectar ao banco
        conn = sqlite3.connect(db_path)
        
        # Salvar no banco
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        print(f"Tabela {table_name} atualizada com {len(df)} registros e {len(df.columns)} colunas.")
    except Exception as e:
        print(f"Erro ao carregar {table_name}: {e}")

if __name__ == "__main__":
    db_path = "/home/ubuntu/datasus-v2/datasus.db"
    load_formatted_csv_to_db("/home/ubuntu/datasus-v2/sih_formatted.csv", "sih_data", db_path)
    load_formatted_csv_to_db("/home/ubuntu/datasus-v2/sia_formatted.csv", "sia_data", db_path)
