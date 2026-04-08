import pandas as pd
import sqlite3
import os
import io

def parse_tabnet_csv(csv_path):
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) < 1000:
        print(f"Aviso: {csv_path} não encontrado ou muito pequeno.")
        return None
    
    try:
        with open(csv_path, 'r', encoding='iso-8859-1') as f:
            lines = f.readlines()
        
        start_idx = -1
        for i, line in enumerate(lines):
            if 'Município' in line or 'Municipio' in line:
                start_idx = i
                break
        
        if start_idx == -1:
            print(f"Aviso: Cabeçalho 'Município' não encontrado em {csv_path}.")
            return None
            
        end_idx = len(lines)
        for i, line in enumerate(lines[start_idx:], start_idx):
            if 'Total' in line or 'Fonte:' in line:
                end_idx = i
                break
                
        data_lines = lines[start_idx:end_idx]
        csv_content = "".join(data_lines)
        
        df = pd.read_csv(io.StringIO(csv_content), sep=';', encoding='iso-8859-1', thousands='.', decimal=',')
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        return df
    except Exception as e:
        print(f"Erro ao processar {csv_path}: {e}")
        return None

def process_and_save(group_name, db_path):
    qtd_path = f"/home/ubuntu/data_{group_name}_qtd.csv"
    valor_path = f"/home/ubuntu/data_{group_name}_valor.csv"
    
    df_qtd = parse_tabnet_csv(qtd_path)
    df_valor = parse_tabnet_csv(valor_path)
    
    if df_qtd is None and df_valor is None:
        print(f"Nenhum dado válido para {group_name}.")
        return

    # Helper para achar a coluna do Município
    def find_mun_col(df):
        for c in df.columns:
            if 'Município' in c or 'Municipio' in c:
                return c
        return None

    if df_qtd is not None and df_valor is not None:
        mun_qtd = find_mun_col(df_qtd)
        mun_valor = find_mun_col(df_valor)
        df_qtd_long = df_qtd.melt(id_vars=[mun_qtd], var_name='Subgrupo', value_name='Quantidade')
        df_valor_long = df_valor.melt(id_vars=[mun_valor], var_name='Subgrupo', value_name='Valor')
        df_qtd_long.rename(columns={mun_qtd: 'Municipio'}, inplace=True)
        df_valor_long.rename(columns={mun_valor: 'Municipio'}, inplace=True)
        df_final = pd.merge(df_qtd_long, df_valor_long, on=['Municipio', 'Subgrupo'], how='outer')
    elif df_qtd is not None:
        mun_qtd = find_mun_col(df_qtd)
        df_final = df_qtd.melt(id_vars=[mun_qtd], var_name='Subgrupo', value_name='Quantidade')
        df_final.rename(columns={mun_qtd: 'Municipio'}, inplace=True)
    else:
        mun_valor = find_mun_col(df_valor)
        df_final = df_valor.melt(id_vars=[mun_valor], var_name='Subgrupo', value_name='Valor')
        df_final.rename(columns={mun_valor: 'Municipio'}, inplace=True)
        
    # Limpeza final
    df_final = df_final[~df_final['Municipio'].str.contains('Total', case=False, na=False)]
    df_final = df_final[~df_final['Subgrupo'].str.contains('Total', case=False, na=False)]
    
    conn = sqlite3.connect(db_path)
    table_name = f"{group_name}_data"
    df_final.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Tabela {table_name} atualizada.")

if __name__ == "__main__":
    db_path = "/home/ubuntu/datasus.db"
    process_and_save("sih", db_path)
    process_and_save("sia", db_path)
