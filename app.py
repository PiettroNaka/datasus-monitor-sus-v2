import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import os

st.set_page_config(page_title="Monitor SUS - DATASUS v2", layout="wide")

st.title("📊 Monitoramento de Produção Hospitalar e Ambulatorial (SUS)")
st.markdown("Dashboard adaptado conforme o fluxo de scraping e validação mensal.")

# Conexão com o banco de dados
db_path = "datasus.db"

def get_connection():
    return sqlite3.connect(db_path)

def load_data(table_name):
    if not os.path.exists(db_path):
        st.error(f"Arquivo de banco de dados '{db_path}' não encontrado.")
        return pd.DataFrame()
    try:
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# Sidebar para navegação
st.sidebar.header("Configurações")
tab_choice = st.sidebar.radio("Navegação:", ["Dashboard", "Validação SQL (Quadro Branco)"])
source = st.sidebar.radio("Fonte de dados:", ["Produção Hospitalar (SIH)", "Produção Ambulatorial (SIA)"])

table_name = "sih_data" if source == "Produção Hospitalar (SIH)" else "sia_data"

if tab_choice == "Dashboard":
    df = load_data(table_name)
    if df.empty:
        st.warning(f"Nenhum dado encontrado para {source}.")
    else:
        st.header(f"📋 Visão Geral - {source}")
        
        # Filtros
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            anos = sorted(df['ANO'].unique().tolist())
            selected_ano = st.multiselect("Filtrar por Ano:", anos, default=anos)
        with col_f2:
            meses = sorted(df['MES'].unique().tolist())
            selected_mes = st.multiselect("Filtrar por Mês:", meses, default=meses)
        with col_f3:
            mun_col = df.columns[0] # Geralmente Município
            municipios = sorted(df[mun_col].unique().tolist())
            selected_mun = st.multiselect("Filtrar por Município:", municipios)

        # Aplicar filtros
        df_filtered = df[df['ANO'].isin(selected_ano) & df['MES'].isin(selected_mes)]
        if selected_mun:
            df_filtered = df_filtered[df_filtered[mun_col].isin(selected_mun)]

        st.dataframe(df_filtered.head(100))

        # Estatísticas e Gráficos
        st.header("📈 Estatísticas e Tendências")
        
        # Agrupar por tempo para gráfico de linha
        trend_df = df_filtered.groupby(['ANO', 'MES'])[['QT_TOTAL', 'VL_TOTAL']].sum().reset_index()
        trend_df['PERIODO'] = trend_df['MES'] + '/' + trend_df['ANO'].astype(str)
        
        c1, c2 = st.columns(2)
        with c1:
            fig_line_qt = px.line(trend_df, x='PERIODO', y='QT_TOTAL', title="Evolução Mensal - Quantidade Total")
            st.plotly_chart(fig_line_qt, use_container_width=True)
        with c2:
            fig_line_vl = px.line(trend_df, x='PERIODO', y='VL_TOTAL', title="Evolução Mensal - Valor Total")
            st.plotly_chart(fig_line_vl, use_container_width=True)

        # Distribuição por Variáveis (QTD_XXXX, VALOR_XXXX)
        st.header("🔍 Detalhamento por Subgrupo")
        var_cols = [c for c in df_filtered.columns if 'QTD_' in c or 'VALOR_' in c or 'VALOR' in c]
        selected_var = st.selectbox("Selecione uma variável para análise:", var_cols)
        
        fig_bar = px.bar(df_filtered.groupby(mun_col)[selected_var].sum().reset_index().nlargest(15, selected_var), 
                         x=mun_col, y=selected_var, title=f"Top 15 Municípios - {selected_var}")
        st.plotly_chart(fig_bar, use_container_width=True)

elif tab_choice == "Validação SQL (Quadro Branco)":
    st.header("📑 Validação de Carga dos Dados")
    st.markdown("Execução da query de validação conforme especificado no quadro branco.")
    
    query = f"""
    SELECT 
        ANO, 
        MES, 
        SUM(QT_TOTAL) as TOTAL_QUANTIDADE, 
        SUM(VL_TOTAL) as TOTAL_VALOR
    FROM {table_name}
    GROUP BY ANO, MES
    ORDER BY ANO, MES
    """
    
    st.code(query, language="sql")
    
    if st.button("Executar Validação"):
        try:
            conn = get_connection()
            val_df = pd.read_sql(query, conn)
            conn.close()
            
            st.success("Validação concluída!")
            st.table(val_df)
            
            st.info("💡 Compare os resultados acima com os totais exibidos no portal TabNet para o mesmo período.")
        except Exception as e:
            st.error(f"Erro ao executar validação: {e}")
