import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime

# Configuração da Página
st.set_page_config(
    page_title="SUS Strategic Intelligence | Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- DATABASE ENGINE ---
db_path = "datasus.db"


def safe_read_sql(query, conn):
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Erro ao executar SQL: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def fetch_analytics_data(table_name):
    if not os.path.exists(db_path):
        return pd.DataFrame()

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        if df.empty:
            return df

        # =========================================================
        # 1) PADRONIZAÇÃO DOS NOMES DAS COLUNAS
        # =========================================================
        df.columns = [str(c).strip() for c in df.columns]

        # Renomeia a primeira coluna geográfica para "Municipio"
        if len(df.columns) > 0:
            primeira_col = df.columns[0]
            if primeira_col != "Municipio":
                df = df.rename(columns={primeira_col: "Municipio"})

        # Padroniza nomes equivalentes
        rename_map = {}

        if "QTD_TOTAL" in df.columns and "QT_TOTAL" not in df.columns:
            rename_map["QTD_TOTAL"] = "QT_TOTAL"

        if "VALOR_TOTAL" in df.columns and "VL_TOTAL" not in df.columns:
            rename_map["VALOR_TOTAL"] = "VL_TOTAL"

        df = df.rename(columns=rename_map)

        # Se existirem as duas versões, consolida em QT_TOTAL e VL_TOTAL
        if "QTD_TOTAL" in df.columns and "QT_TOTAL" in df.columns:
            df["QT_TOTAL"] = pd.to_numeric(df["QT_TOTAL"], errors="coerce")
            df["QTD_TOTAL"] = pd.to_numeric(df["QTD_TOTAL"], errors="coerce")
            df["QT_TOTAL"] = df["QT_TOTAL"].fillna(df["QTD_TOTAL"])
            df = df.drop(columns=["QTD_TOTAL"])

        if "VALOR_TOTAL" in df.columns and "VL_TOTAL" in df.columns:
            df["VL_TOTAL"] = pd.to_numeric(df["VL_TOTAL"], errors="coerce")
            df["VALOR_TOTAL"] = pd.to_numeric(df["VALOR_TOTAL"], errors="coerce")
            df["VL_TOTAL"] = df["VL_TOTAL"].fillna(df["VALOR_TOTAL"])
            df = df.drop(columns=["VALOR_TOTAL"])

        # =========================================================
        # 2) GARANTIA DAS COLUNAS OBRIGATÓRIAS
        # =========================================================
        required_cols = ["Municipio", "ANO", "MES", "QT_TOTAL", "VL_TOTAL"]
        for col in required_cols:
            if col not in df.columns:
                st.error(f"Coluna obrigatória ausente: {col}")
                return pd.DataFrame()

        # =========================================================
        # 3) LIMPEZA ROBUSTA DE NÚMEROS
        # =========================================================
        def limpar_numero_br(x):
            if pd.isna(x):
                return pd.NA

            s = str(x).strip()

            if s == "" or s.lower() in ["nan", "none", "null", "-", "--"]:
                return pd.NA

            s = s.replace(" ", "")

            # padrão BR: 1.234,56
            if "." in s and "," in s:
                s = s.replace(".", "").replace(",", ".")
            # decimal BR: 123,45
            elif "," in s:
                s = s.replace(",", ".")

            try:
                return float(s)
            except Exception:
                return pd.NA

        # Limpa colunas principais
        df["ANO"] = df["ANO"].apply(limpar_numero_br)
        df["QT_TOTAL"] = df["QT_TOTAL"].apply(limpar_numero_br)
        df["VL_TOTAL"] = df["VL_TOTAL"].apply(limpar_numero_br)

        # Limpa demais colunas numéricas, exceto metadados
        meta_cols = ["Municipio", "ANO", "MES"]
        for col in df.columns:
            if col not in meta_cols:
                df[col] = df[col].apply(limpar_numero_br)

        # Conversão final
        df["ANO"] = pd.to_numeric(df["ANO"], errors="coerce")
        df["QT_TOTAL"] = pd.to_numeric(df["QT_TOTAL"], errors="coerce")
        df["VL_TOTAL"] = pd.to_numeric(df["VL_TOTAL"], errors="coerce")

        # Regras de saneamento
        df.loc[df["QT_TOTAL"] < 0, "QT_TOTAL"] = pd.NA
        df.loc[df["VL_TOTAL"] < 0, "VL_TOTAL"] = pd.NA

        df["QT_TOTAL"] = df["QT_TOTAL"].fillna(0)
        df["VL_TOTAL"] = df["VL_TOTAL"].fillna(0)

        # =========================================================
        # 4) PADRONIZAÇÃO DE MÊS
        # =========================================================
        df["MES"] = df["MES"].astype(str).str.strip()

        month_map = {
            "Jan": 1, "Janeiro": 1, "1": 1, "01": 1,
            "Fev": 2, "Fevereiro": 2, "2": 2, "02": 2,
            "Mar": 3, "Março": 3, "Marco": 3, "3": 3, "03": 3,
            "Abr": 4, "Abril": 4, "4": 4, "04": 4,
            "Mai": 5, "Maio": 5, "5": 5, "05": 5,
            "Jun": 6, "Junho": 6, "6": 6, "06": 6,
            "Jul": 7, "Julho": 7, "7": 7, "07": 7,
            "Ago": 8, "Agosto": 8, "8": 8, "08": 8,
            "Set": 9, "Setembro": 9, "9": 9, "09": 9,
            "Out": 10, "Outubro": 10, "10": 10,
            "Nov": 11, "Novembro": 11, "11": 11,
            "Dez": 12, "Dezembro": 12, "12": 12
        }

        df["MES_PAD"] = df["MES"].astype(str).str.strip().str.title()
        df["MONTH_NUM"] = df["MES_PAD"].map(month_map)

        # fallback: se MES já vier numérico
        mask_month_null = df["MONTH_NUM"].isna()
        if mask_month_null.any():
            df.loc[mask_month_null, "MONTH_NUM"] = pd.to_numeric(
                df.loc[mask_month_null, "MES"], errors="coerce"
            )

        df["MONTH_NUM"] = pd.to_numeric(df["MONTH_NUM"], errors="coerce")

        # =========================================================
        # 5) CRIAÇÃO DA DATA
        # =========================================================
        df = df.dropna(subset=["ANO", "MONTH_NUM"]).copy()

        df["ANO"] = df["ANO"].astype(int)
        df["MONTH_NUM"] = df["MONTH_NUM"].astype(int)

        df["DATE"] = pd.to_datetime(
            df["ANO"].astype(str) + "-" + df["MONTH_NUM"].astype(str).str.zfill(2) + "-01",
            errors="coerce"
        )

        df = df.dropna(subset=["DATE"]).copy()

        # =========================================================
        # 6) LIMPEZA FINAL
        # =========================================================
        df["Municipio"] = df["Municipio"].astype(str).str.strip()

        df = df[
            (df["Municipio"] != "") &
            (df["Municipio"].str.lower() != "nan")
        ].copy()

        df = df.sort_values("DATE").reset_index(drop=True)

        return df

    except Exception as e:
        st.error(f"Engine Error: {e}")
        return pd.DataFrame()

    finally:
        if conn is not None:
            conn.close()


# --- SIDEBAR & FILTERS ---
st.sidebar.title("🏥 Controle Estratégico")
st.sidebar.markdown("---")

source_opt = st.sidebar.radio("Unidade de Negócios:", ["Hospitalar (SIH)", "Ambulatorial (SIA)"])
table_name = "sih_data" if "SIH" in source_opt else "sia_data"

raw_df = fetch_analytics_data(table_name)

# DEBUG opcional
with st.expander("DEBUG - Diagnóstico da base", expanded=False):
    st.write("Colunas:", raw_df.columns.tolist() if not raw_df.empty else [])
    if not raw_df.empty:
        st.write("Tipos:")
        st.dataframe(pd.DataFrame({
            "coluna": raw_df.columns,
            "dtype": [str(raw_df[c].dtype) for c in raw_df.columns]
        }), use_container_width=True)

        st.write("Primeiras 20 linhas:")
        st.dataframe(raw_df.head(20), use_container_width=True)

        if "MES" in raw_df.columns:
            st.write("Valores únicos de MES:")
            st.write(sorted(raw_df["MES"].astype(str).unique().tolist()))

        for col in ["QT_TOTAL", "VL_TOTAL"]:
            if col in raw_df.columns:
                st.write(f"Amostra bruta de {col}:")
                st.write(raw_df[col].astype(str).head(20).tolist())

                st.write(f"Resumo de {col}:")
                st.write({
                    "nulos": int(raw_df[col].isna().sum()),
                    "min": pd.to_numeric(raw_df[col], errors="coerce").min(),
                    "max": pd.to_numeric(raw_df[col], errors="coerce").max(),
                })

if raw_df.empty:
    st.warning("Sincronização do banco em andamento ou dados indisponíveis.")
    st.stop()

mun_col = "Municipio"

# Filtros dinâmicos
with st.sidebar.expander("📅 Janela de tempo", expanded=True):
    anos = sorted(pd.Series(raw_df["ANO"].dropna().unique()).tolist())
    default_anos = anos[-1:] if anos else []
    sel_anos = st.multiselect("Anos", anos, default=default_anos)

    meses = sorted(pd.Series(raw_df["MES"].dropna().astype(str).unique()).tolist())
    sel_meses = st.multiselect("Meses", meses, default=meses)

with st.sidebar.expander("📍 Âmbito Geográfico", expanded=False):
    muns = sorted(raw_df[mun_col].dropna().astype(str).unique().tolist())
    sel_muns = st.multiselect("Municípios", muns)

# Filtering
df = raw_df.copy()

if sel_anos:
    df = df[df["ANO"].isin(sel_anos)]

if sel_meses:
    df = df[df["MES"].astype(str).isin(sel_meses)]

if sel_muns:
    df = df[df[mun_col].astype(str).isin(sel_muns)]

if df.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
    st.stop()

# --- MAIN DASHBOARD ---
st.title("📊 Monitoramento Estratégico SUS")
st.caption(f"Última sincronização: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

tab_exec, tab_subgroups, tab_geospatial, tab_validation = st.tabs([
    "🚀 Visão Executiva",
    "🧩 Decomposição de subgrupos",
    "🗺️ Inteligência Regional",
    "⚙️ Auditoria Técnica"
])

# 1. EXECUTIVE OVERVIEW
with tab_exec:
    st.subheader("Desempenho Financeiro e Operacional")

    last_month = df["DATE"].max()
    prev_month = last_month - pd.DateOffset(months=1) if pd.notna(last_month) else None

    curr_metrics = df[df["DATE"] == last_month][["VL_TOTAL", "QT_TOTAL"]].sum()

    if prev_month is not None:
        prev_metrics = df[df["DATE"] == prev_month][["VL_TOTAL", "QT_TOTAL"]].sum()
    else:
        prev_metrics = pd.Series({"VL_TOTAL": 0, "QT_TOTAL": 0})

    def calc_delta(curr, prev):
        if pd.isna(prev) or prev == 0:
            return 0
        return ((curr - prev) / prev) * 100

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    total_val = pd.to_numeric(df["VL_TOTAL"], errors="coerce").fillna(0).sum()
    total_qty = pd.to_numeric(df["QT_TOTAL"], errors="coerce").fillna(0).sum()
    avg_cost = total_val / total_qty if total_qty > 0 else 0
    unique_muns = df[mun_col].nunique()

    kpi_col1.metric(
        "Investimento Total",
        f"R$ {total_val/1e6:.1f}M",
        f"{calc_delta(curr_metrics['VL_TOTAL'], prev_metrics['VL_TOTAL']):.1f}% vs mês ant."
    )

    kpi_col2.metric(
        "Total de Procedimentos",
        f"{total_qty/1e3:.1f}k",
        f"{calc_delta(curr_metrics['QT_TOTAL'], prev_metrics['QT_TOTAL']):.1f}% vs mês ant."
    )

    kpi_col3.metric("Ticket médio (Custo/Proc)", f"R$ {avg_cost:,.2f}")
    kpi_col4.metric("Abrangência (Municípios)", f"{unique_muns}")

    st.divider()

    st.subheader("Análise de Série Temporal")
    ts_df = df.groupby("DATE")[["VL_TOTAL", "QT_TOTAL"]].sum().reset_index()

    fig_ts = go.Figure()
    fig_ts.add_trace(
        go.Scatter(
            x=ts_df["DATE"],
            y=ts_df["VL_TOTAL"],
            name="Investimento (R$)",
            line=dict(color="#00d4ff", width=3),
            fill="tozeroy"
        )
    )
    fig_ts.add_trace(
        go.Bar(
            x=ts_df["DATE"],
            y=ts_df["QT_TOTAL"],
            name="Volume (QTD)",
            yaxis="y2",
            opacity=0.4,
            marker_color="#ff7f0e"
        )
    )

    fig_ts.update_layout(
        template="plotly_white",
        yaxis=dict(title="Investimento (R$)"),
        yaxis2=dict(title="Volume", overlaying="y", side="right"),
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
        val_cols = [c for c in df.columns if "VALOR_" in c and "TOTAL" not in c]

        if val_cols:
            sub_df = df[val_cols].sum().reset_index()
            sub_df.columns = ["Subgrupo", "Valor"]
            sub_df["Valor"] = pd.to_numeric(sub_df["Valor"], errors="coerce").fillna(0)
            sub_df = sub_df[sub_df["Valor"] > 0].copy()
            sub_df["Label"] = sub_df["Subgrupo"].str.replace("VALOR_", "", regex=False)

            if not sub_df.empty:
                fig_tree = px.treemap(
                    sub_df,
                    path=["Label"],
                    values="Valor",
                    color="Valor",
                    color_continuous_scale="Blues"
                )
                st.plotly_chart(fig_tree, use_container_width=True)
            else:
                st.warning("Os subgrupos existem, mas estão sem valores positivos para exibição.")
        else:
            st.warning("Dados de subgrupos indisponíveis para esta fonte.")

    with col_sub2:
        st.markdown("**Análise de Pareto (80/20)**")
        if val_cols:
            sub_df = df[val_cols].sum().reset_index()
            sub_df.columns = ["Subgrupo", "Valor"]
            sub_df["Valor"] = pd.to_numeric(sub_df["Valor"], errors="coerce").fillna(0)
            sub_df = sub_df[sub_df["Valor"] > 0].copy()
            sub_df["Label"] = sub_df["Subgrupo"].str.replace("VALOR_", "", regex=False)

            if not sub_df.empty and sub_df["Valor"].sum() > 0:
                sub_df = sub_df.sort_values("Valor", ascending=False)
                sub_df["Cum_Perc"] = 100 * sub_df["Valor"].cumsum() / sub_df["Valor"].sum()

                fig_pareto = go.Figure()
                fig_pareto.add_trace(
                    go.Bar(
                        x=sub_df["Label"],
                        y=sub_df["Valor"],
                        name="Valor Individual",
                        marker_color="#00d4ff"
                    )
                )
                fig_pareto.add_trace(
                    go.Scatter(
                        x=sub_df["Label"],
                        y=sub_df["Cum_Perc"],
                        name="% Acumulado",
                        yaxis="y2",
                        line=dict(color="#ff4b4b", width=3)
                    )
                )

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
        else:
            st.warning("Dados de Pareto indisponíveis.")

# 3. GEOSPATIAL INTELLIGENCE
with tab_geospatial:
    st.subheader("Performance Regional e Eficiência")

    col_geo1, col_geo2 = st.columns(2)

    with col_geo1:
        st.markdown("**Top 15 Municípios (Maior Investimento)**")
        top_mun = (
            df.groupby(mun_col, dropna=False)["VL_TOTAL"]
            .sum()
            .nlargest(15)
            .reset_index()
        )

        if not top_mun.empty:
            fig_mun = px.bar(
                top_mun,
                x="VL_TOTAL",
                y=mun_col,
                orientation="h",
                color="VL_TOTAL",
                color_continuous_scale="GnBu"
            )
            fig_mun.update_layout(
                yaxis={"categoryorder": "total ascending"},
                margin=dict(t=0, l=0, r=0, b=0)
            )
            st.plotly_chart(fig_mun, use_container_width=True)
        else:
            st.warning("Sem dados para o ranking de municípios.")

    with col_geo2:
        st.markdown("**Matriz de Eficiência (Volume vs Investimento)**")

        eff_df = df.groupby(mun_col, dropna=False)[["QT_TOTAL", "VL_TOTAL"]].sum().reset_index()

        eff_df["QT_TOTAL"] = pd.to_numeric(eff_df["QT_TOTAL"], errors="coerce")
        eff_df["VL_TOTAL"] = pd.to_numeric(eff_df["VL_TOTAL"], errors="coerce")

        eff_df["Custo_Medio"] = eff_df["VL_TOTAL"] / eff_df["QT_TOTAL"].replace(0, pd.NA)
        eff_df = eff_df.dropna(subset=["QT_TOTAL", "VL_TOTAL", "Custo_Medio"]).copy()
        eff_df = eff_df[
            (eff_df["QT_TOTAL"] > 0) &
            (eff_df["VL_TOTAL"] >= 0) &
            (eff_df["Custo_Medio"] >= 0)
        ].copy()

        if not eff_df.empty:
            fig_scatter = px.scatter(
                eff_df,
                x="QT_TOTAL",
                y="VL_TOTAL",
                size="Custo_Medio",
                hover_name=mun_col,
                color="Custo_Medio",
                color_continuous_scale="YlOrRd"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.warning("Não há dados válidos para a matriz de eficiência.")

# 4. TECHNICAL AUDIT
with tab_validation:
    st.subheader("Auditoria de Integridade dos Dados")
    st.info("💡 Este módulo permite a auditoria direta da base carregada.")

    col_audit1, col_audit2 = st.columns(2)

    with col_audit1:
        st.markdown("**Log de Auditoria SQL**")

        audit_query = f"""
        SELECT
            ANO,
            MES,
            SUM(QT_TOTAL) as QTY,
            SUM(VL_TOTAL) as VALUE
        FROM {table_name}
        GROUP BY ANO, MES
        ORDER BY ANO, MES
        """
        st.code(audit_query, language="sql")

        if st.button("Executar Auditoria"):
            conn = None
            try:
                conn = sqlite3.connect(db_path)
                audit_df = pd.read_sql(audit_query, conn)
                st.dataframe(audit_df, use_container_width=True)
            except Exception as e:
                st.error(f"Erro na auditoria: {e}")
            finally:
                if conn is not None:
                    conn.close()

    with col_audit2:
        st.markdown("**Amostra de Dados Brutos (Primeiras 1000 linhas)**")
        st.dataframe(df.head(1000), use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.caption("Desenvolvido por Data Engine")
