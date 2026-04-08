import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os

st.set_page_config(page_title="Monitor SUS Analytics", layout="wide", initial_sidebar_state="expanded")

# Estilo CSS customizado para um visual mais "Data Science"
st.markdown("""
    <style>
    .main {
        background-color: #f8f9fa;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    </style>
    """, unsafe_allow_index=True)

# Conexão com o banco de dados
db_path = "datasus.db"

def get_connection():
    return sqlite3.connect(db_path)

@st.cache_data
def load_data(table_name):
    if not os.path.exists(db_path):
        return pd.DataFrame()
    try:
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        
        # Garantir conversão numérica
        meta_cols = ['Municipio', 'ANO', 'MES']
        if not df.empty and df.columns[0] not in meta_cols:
            meta_cols[0] = df.columns[0]
            
        for col in df.columns:
            if col not in meta_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# Sidebar
st.sidebar.title("📊 Monitor SUS Analytics")
st.sidebar.markdown("---")
tab_choice = st.sidebar.radio("Navegação:", ["Dashboard Executivo", "Análise de Pareto", "Validação Técnica"])
source = st.sidebar.radio("Fonte de dados:", ["Produção Hospitalar (SIH)", "Produção Ambulatorial (SIA)"])

table_name = "sih_data" if source == "Produção Hospitalar (SIH)" else "sia_data"
df = load_data(table_name)

if df.empty:
    st.warning(f"Aguardando extração completa dos dados de {source}...")
else:
    mun_col = df.columns[0]
    
    # Filtros Globais
    st.sidebar.markdown("---")
    anos = sorted(df['ANO'].unique().tolist())
    selected_ano = st.sidebar.multiselect("Anos:", anos, default=anos)
    meses = sorted(df['MES'].unique().tolist())
    selected_mes = st.sidebar.multiselect("Meses:", meses, default=meses)
    
    df_filtered = df[df['ANO'].isin(selected_ano) & df['MES'].isin(selected_mes)]

    if tab_choice == "Dashboard Executivo":
        st.title(f"📈 Dashboard Executivo - {source}")
        
        # KPIs no Topo
        total_invest = df_filtered['VL_TOTAL'].sum()
        total_qtd = df_filtered['QT_TOTAL'].sum()
        ticket_medio = total_invest / total_qtd if total_qtd > 0 else 0
        
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("Investimento Total", f"R$ {total_invest:,.2f}")
        kpi2.metric("Total de Procedimentos", f"{int(total_qtd):,}")
        kpi3.metric("Custo Médio p/ Procedimento", f"R$ {ticket_medio:,.2f}")
        
        st.markdown("---")
        
        # Tendência Temporal
        st.subheader("📅 Evolução Mensal de Custos e Volume")
        trend_df = df_filtered.groupby(['ANO', 'MES'])[['QT_TOTAL', 'VL_TOTAL']].sum().reset_index()
        # Ordenação correta por mês/ano
        month_map = {'Jan':1, 'Fev':2, 'Mar':3, 'Abr':4, 'Mai':5, 'Jun':6, 
                     'Jul':7, 'Ago':8, 'Set':9, 'Out':10, 'Nov':11, 'Dez':12}
        trend_df['MONTH_NUM'] = trend_df['MES'].map(month_map)
        trend_df = trend_df.sort_values(['ANO', 'MONTH_NUM'])
        trend_df['PERIODO'] = trend_df['MES'] + '/' + trend_df['ANO'].astype(str)
        
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(x=trend_df['PERIODO'], y=trend_df['VL_TOTAL'], name='Valor Total', line=dict(color='#1f77b4', width=4)))
        fig_trend.add_trace(go.Bar(x=trend_df['PERIODO'], y=trend_df['QT_TOTAL'], name='Quantidade', yaxis='y2', opacity=0.3, marker_color='#ff7f0e'))
        
        fig_trend.update_layout(
            title="Série Histórica: Investimento vs. Volume",
            yaxis=dict(title="Valor (R$)"),
            yaxis2=dict(title="Quantidade", overlaying='y', side='right'),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            template="plotly_white"
        )
        st.plotly_chart(fig_trend, use_container_width=True)
        
        # Análise Geográfica (Top Municípios)
        col_geo1, col_geo2 = st.columns(2)
        with col_geo1:
            st.subheader("📍 Top 10 Municípios por Investimento")
            top_mun = df_filtered.groupby(mun_col)['VL_TOTAL'].sum().nlargest(10).reset_index()
            fig_mun = px.bar(top_mun, x='VL_TOTAL', y=mun_col, orientation='h', color='VL_TOTAL', color_continuous_scale='Blues')
            fig_mun.update_layout(showlegend=False, yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_mun, use_container_width=True)
            
        with col_geo2:
            st.subheader("🔥 Concentração de Custos")
            # Heatmap de Sazonalidade
            pivot_heat = df_filtered.pivot_table(index='MES', columns='ANO', values='VL_TOTAL', aggfunc='sum')
            pivot_heat = pivot_heat.reindex(['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'])
            fig_heat = px.imshow(pivot_heat, labels=dict(x="Ano", y="Mês", color="Valor"), color_continuous_scale='RdYlGn_r')
            st.plotly_chart(fig_heat, use_container_width=True)

    elif tab_choice == "Análise de Pareto":
        st.title("🎯 Análise de Pareto (Regra 80/20)")
        st.markdown("Identificação dos subgrupos que representam a maior parte do investimento.")
        
        # Preparar dados para Pareto
        var_cols = [c for c in df_filtered.columns if 'VALOR_' in c and 'TOTAL' not in c]
        pareto_df = df_filtered[var_cols].sum().reset_index()
        pareto_df.columns = ['Subgrupo', 'Valor']
        pareto_df = pareto_df.sort_values('Valor', ascending=False)
        pareto_df['Cum_Sum'] = pareto_df['Valor'].cumsum()
        pareto_df['Cum_Perc'] = 100 * pareto_df['Cum_Sum'] / pareto_df['Valor'].sum()
        
        fig_pareto = go.Figure()
        fig_pareto.add_trace(go.Bar(x=pareto_df['Subgrupo'], y=pareto_df['Valor'], name="Valor Individual", marker_color='#1f77b4'))
        fig_pareto.add_trace(go.Scatter(x=pareto_df['Subgrupo'], y=pareto_df['Cum_Perc'], name="% Acumulada", yaxis="y2", line=dict(color="#d62728", width=3)))
        
        fig_pareto.update_layout(
            title="Curva de Pareto por Subgrupo de Procedimento",
            yaxis=dict(title="Valor (R$)"),
            yaxis2=dict(title="Percentual Acumulado (%)", overlaying="y", side="right", range=[0, 105]),
            template="plotly_white"
        )
        st.plotly_chart(fig_pareto, use_container_width=True)
        
        st.info("💡 Os subgrupos à esquerda da linha de 80% são os que exigem maior atenção na gestão orçamentária.")

    elif tab_choice == "Validação Técnica":
        st.title("⚙️ Validação Técnica de Dados")
        
        # Query do Quadro Branco
        st.subheader("Query de Validação (Quadro Branco)")
        query = f"SELECT ANO, MES, SUM(QT_TOTAL) as QT_TOTAL, SUM(VL_TOTAL) as VL_TOTAL FROM {table_name} GROUP BY ANO, MES ORDER BY ANO, MES"
        st.code(query, language="sql")
        
        if st.button("Executar Query"):
            conn = get_connection()
            val_df = pd.read_sql(query, conn)
            conn.close()
            st.table(val_df)
            
        st.markdown("---")
        st.subheader("Amostra dos Dados Brutos")
        st.dataframe(df_filtered.head(500))
