import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime

# Configuração da Página - Estabilidade Máxima
st.set_page_config(
    page_title="SUS Strategic Intelligence | Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- DATABASE ENGINE ---
db_path = "datasus.db"

@st.cache_data(ttl=600)
def fetch_analytics_data(table_name):
    if not os.path.exists(db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        
        # Data Cleaning & Type Casting
        meta_cols = ['Municipio', 'ANO', 'MES']
        if not df.empty and df.columns[0] not in meta_cols:
            meta_cols[0] = df.columns[0]
            
        for col in df.columns:
            if col not in meta_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Time Dimension Creation
        month_map = {'Jan':1, 'Fev':2, 'Mar':3, 'Abr':4, 'Mai':5, 'Jun':6, 
                     'Jul':7, 'Ago':8, 'Set':9, 'Out':10, 'Nov':11, 'Dez':12}
        df['MONTH_NUM'] = df['MES'].map(month_map)
        df['DATE'] = pd.to_datetime(df['ANO'].astype(str) + '-' + df['MONTH_NUM'].astype(str) + '-01')
        return df.sort_values('DATE')
    except Exception as e:
        st.error(f"Engine Error: {e}")
        return pd.DataFrame()

# --- SIDEBAR & FILTERS ---
st.sidebar.title("🏥 Strategic Control")
st.sidebar.markdown("---")

source_opt = st.sidebar.radio("Business Unit:", ["Hospitalar (SIH)", "Ambulatorial (SIA)"])
table_name = "sih_data" if "SIH" in source_opt else "sia_data"

raw_df = fetch_analytics_data(table_name)

if raw_df.empty:
    st.warning("Database synchronization in progress...")
    st.stop()

# Filtros Dinâmicos
with st.sidebar.expander("📅 Time Window", expanded=True):
    anos = sorted(raw_df['ANO'].unique().tolist())
    sel_anos = st.multiselect("Anos", anos, default=anos[-1:])
    meses = sorted(raw_df['MES'].unique().tolist())
    sel_meses = st.multiselect("Meses", meses, default=meses)

with st.sidebar.expander("📍 Geographic Scope", expanded=False):
    mun_col = raw_df.columns[0]
    muns = sorted(raw_df[mun_col].unique().tolist())
    sel_muns = st.multiselect("Municípios", muns)

# Data Filtering
df = raw_df[raw_df['ANO'].isin(sel_anos) & raw_df['MES'].isin(sel_meses)]
if sel_muns:
    df = df[df[mun_col].isin(sel_muns)]

# --- MAIN DASHBOARD ---
st.title("📊 Monitoramento Estratégico SUS")
st.caption(f"Última sincronização: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

tab_exec, tab_subgroups, tab_geospatial, tab_validation = st.tabs([
    "🚀 Visão Executiva", 
    "🧩 Decomposição de Subgrupos", 
    "🗺️ Inteligência Regional",
    "⚙️ Auditoria Técnica"
])

# 1. EXECUTIVE OVERVIEW
with tab_exec:
    st.subheader("Performance Financeira e Operacional")
    
    # KPIs com Cálculo de Delta (vs Mês Anterior)
    last_month = df['DATE'].max()
    prev_month = last_month - pd.DateOffset(months=1)
    
    curr_metrics = df[df['DATE'] == last_month][['VL_TOTAL', 'QT_TOTAL']].sum()
    prev_metrics = df[df['DATE'] == prev_month][['VL_TOTAL', 'QT_TOTAL']].sum()
    
    def calc_delta(curr, prev):
        if prev == 0: return 0
        return ((curr - prev) / prev) * 100

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    
    total_val = df['VL_TOTAL'].sum()
    kpi_col1.metric("Investimento Total", f"R$ {total_val/1e6:.1f}M", 
                    f"{calc_delta(curr_metrics['VL_TOTAL'], prev_metrics['VL_TOTAL']):.1f}% vs mês ant.")
    
    total_qty = df['QT_TOTAL'].sum()
    kpi_col2.metric("Total Procedimentos", f"{total_qty/1e3:.1f}k", 
                    f"{calc_delta(curr_metrics['QT_TOTAL'], prev_metrics['QT_TOTAL']):.1f}% vs mês ant.")
    
    avg_cost = total_val / total_qty if total_qty > 0 else 0
    kpi_col3.metric("Ticket Médio (Custo/Proc)", f"R$ {avg_cost:,.2f}")
    
    unique_muns = df[mun_col].nunique()
    kpi_col4.metric("Abrangência (Municípios)", f"{unique_muns}")

    st.divider()

    # Gráfico de Tendência Combinado (Dual Axis)
    st.subheader("Análise de Série Temporal")
    ts_df = df.groupby('DATE')[['VL_TOTAL', 'QT_TOTAL']].sum().reset_index()
    
    fig_ts = go.Figure()
    fig_ts.add_trace(go.Scatter(x=ts_df['DATE'], y=ts_df['VL_TOTAL'], name='Investimento (R$)', 
                                line=dict(color='#00d4ff', width=3), fill='tozeroy'))
    fig_ts.add_trace(go.Bar(x=ts_df['DATE'], y=ts_df['QT_TOTAL'], name='Volume (QTD)', 
                            yaxis='y2', opacity=0.4, marker_color='#ff7f0e'))
    
    fig_ts.update_layout(
        template="plotly_white",
        yaxis=dict(title="Investimento (R$)"),
        yaxis2=dict(title="Volume", overlaying='y', side='right'),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=40, b=20),
        height=450
    )
    st.plotly_chart(fig_ts, use_container_width=True)

# 2. SUBGROUP DECOMPOSITION
with tab_subgroups:
    st.subheader("Pareto e Hierarquia de Subgrupos")
    
    col_sub1, col_sub2 = st.columns(2)
    
    with col_sub1:
        st.markdown("**Distribuição por Treemap**")
        val_cols = [c for c in df.columns if 'VALOR_' in c and 'TOTAL' not in c]
        if val_cols:
            sub_df = df[val_cols].sum().reset_index()
            sub_df.columns = ['Subgrupo', 'Valor']
            sub_df['Label'] = sub_df['Subgrupo'].str.replace('VALOR_', '')
            
            fig_tree = px.treemap(sub_df, path=['Label'], values='Valor', 
                                  color='Valor', color_continuous_scale='Blues')
            st.plotly_chart(fig_tree, use_container_width=True)
        else:
            st.warning("Dados de subgrupos indisponíveis para esta fonte.")

    with col_sub2:
        st.markdown("**Análise de Pareto (80/20)**")
        if val_cols:
            sub_df = sub_df.sort_values('Valor', ascending=False)
            sub_df['Cum_Perc'] = 100 * sub_df['Valor'].cumsum() / sub_df['Valor'].sum()
            
            fig_pareto = go.Figure()
            fig_pareto.add_trace(go.Bar(x=sub_df['Label'], y=sub_df['Valor'], name="Valor Individual", marker_color='#00d4ff'))
            fig_pareto.add_trace(go.Scatter(x=sub_df['Label'], y=sub_df['Cum_Perc'], name="% Acumulado", 
                                            yaxis="y2", line=dict(color="#ff4b4b", width=3)))
            
            fig_pareto.update_layout(
                template="plotly_white",
                yaxis2=dict(overlaying="y", side="right", range=[0, 105]),
                margin=dict(t=0, l=0, r=0, b=0),
                height=400,
                showlegend=False
            )
            st.plotly_chart(fig_pareto, use_container_width=True)
        else:
            st.warning("Dados de Pareto indisponíveis.")

# 3. GEOSPATIAL INTELLIGENCE
with tab_geospatial:
    st.subheader("Performance Regional e Eficiência")
    
    col_geo1, col_geo2 = st.columns(2)
    
    with col_geo1:
        st.markdown("**Top 15 Municípios (Maior Investimento)**")
        top_mun = df.groupby(mun_col)['VL_TOTAL'].sum().nlargest(15).reset_index()
        fig_mun = px.bar(top_mun, x='VL_TOTAL', y=mun_col, orientation='h', 
                         color='VL_TOTAL', color_continuous_scale='GnBu')
        fig_mun.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(fig_mun, use_container_width=True)
        
    with col_geo2:
        st.markdown("**Matriz de Eficiência (Volume vs Investimento)**")
        eff_df = df.groupby(mun_col)[['QT_TOTAL', 'VL_TOTAL']].sum().reset_index()
        eff_df['Custo_Medio'] = eff_df['VL_TOTAL'] / eff_df['QT_TOTAL']
        
        fig_scatter = px.scatter(eff_df, x='QT_TOTAL', y='VL_TOTAL', size='Custo_Medio', 
                                 hover_name=mun_col, color='Custo_Medio', 
                                 color_continuous_scale='YlOrRd')
        st.plotly_chart(fig_scatter, use_container_width=True)

# 4. TECHNICAL AUDIT
with tab_validation:
    st.subheader("Auditoria de Integridade dos Dados")
    st.info("💡 Este módulo permite a auditoria direta conforme o fluxo do quadro branco.")
    
    col_audit1, col_audit2 = st.columns(2)
    
    with col_audit1:
        st.markdown("**Log de Auditoria SQL**")
        audit_query = f"SELECT ANO, MES, SUM(QT_TOTAL) as QTY, SUM(VL_TOTAL) as VALUE FROM {table_name} GROUP BY ANO, MES ORDER BY DATE"
        st.code(audit_query, language="sql")
        
        if st.button("Executar Auditoria"):
            conn = sqlite3.connect(db_path)
            audit_df = pd.read_sql(audit_query, conn)
            conn.close()
            st.dataframe(audit_df, use_container_width=True)

    with col_audit2:
        st.markdown("**Amostra de Dados Brutos (Primeiras 1000 linhas)**")
        st.dataframe(df.head(1000), use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.caption("Powered by Manus Data Engine")
