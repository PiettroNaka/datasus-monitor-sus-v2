import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import os

st.set_page_config(page_title="Monitor SUS - DATASUS", layout="wide")

st.title("📊 Monitoramento de Produção Hospitalar e Ambulatorial (SUS)")
st.markdown("Dashboard desenvolvido com dados parciais extraídos do portal DATASUS TabNet.")

# Conexão com o banco de dados
db_path = "/home/ubuntu/datasus.db"

def load_data(table_name):
    if not os.path.exists(db_path):
        st.error(f"Arquivo de banco de dados '{db_path}' não encontrado.")
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# Sidebar para navegação
st.sidebar.header("Configurações")
source = st.sidebar.radio("Selecione a fonte de dados:", ["Produção Hospitalar (SIH)", "Produção Ambulatorial (SIA)"])

table_name = "sih_data" if source == "Produção Hospitalar (SIH)" else "sia_data"
df = load_data(table_name)

if df.empty:
    st.warning(f"Nenhum dado encontrado para {source}. Devido à instabilidade do portal DATASUS, alguns dados não puderam ser extraídos.")
else:
    # 3.1 Lista dos dados armazenados
    st.header(f"📋 Lista de Dados - {source}")
    st.write(f"Total de registros: {len(df)}")
    
    # Filtro por Município
    municipios = sorted(df['Municipio'].unique().tolist())
    selected_mun = st.multiselect("Filtrar por Município:", municipios)
    
    df_filtered = df if not selected_mun else df[df['Municipio'].isin(selected_mun)]
    st.dataframe(df_filtered.head(100))

    # 3.2 Estatísticas descritivas
    st.header("📈 Estatísticas Descritivas")
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    if num_cols:
        st.write(df[num_cols].describe())
    else:
        st.info("Nenhuma coluna numérica disponível no momento.")

    # 3.3 Diversos gráficos
    st.header("📊 Visualizações")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if num_cols:
            metric = st.selectbox("Métrica para o Top 10:", num_cols, key="bar_metric")
            top_df = df.groupby('Municipio')[metric].sum().reset_index().nlargest(10, metric)
            fig1 = px.bar(top_df, x='Municipio', y=metric, title=f"Top 10 Municípios por {metric}", color=metric)
            st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        if 'Subgrupo' in df.columns and num_cols:
            pie_metric = st.selectbox("Métrica para Subgrupos:", num_cols, key="pie_metric")
            sub_df = df.groupby('Subgrupo')[pie_metric].sum().reset_index()
            fig2 = px.pie(sub_df, values=pie_metric, names='Subgrupo', title=f"Distribuição por Subgrupo ({pie_metric})")
            st.plotly_chart(fig2, use_container_width=True)

    # Nota sobre dados parciais
    st.info("⚠️ Nota: Devido à instabilidade técnica no portal DATASUS TabNet, alguns indicadores (como Quantidade ou Valor) podem estar ausentes para certas fontes de dados.")
