import streamlit as st
import pandas as pd
import plotly.express as px

from analytics import (
    get_funnel_daily,
    get_revenue_daily,
    get_product_performance,
    get_campaign_performance,
    compute_funnel_kpis,
)

st.set_page_config(
    page_title="E-commerce Analytics",
    layout="wide",
)


# ------------------------------
# Sidebar / Layout
# ------------------------------
st.sidebar.title("E-commerce Analytics")
st.sidebar.markdown(
    """
Pipeline: **n8n → S3 → Airflow → Snowflake → dbt → Streamlit**

Camada atual: **GOLD (Analytics)**.
"""
)

page = st.sidebar.radio(
    "Navegação",
    ["Overview", "Produtos", "Campanhas"],
)


# ------------------------------
# Página: Overview
# ------------------------------
if page == "Overview":
    st.title("📊 Visão Geral do E-commerce")

    # Carrega dados principais
    df_funnel = get_funnel_daily()
    df_revenue = get_revenue_daily()

    kpis = compute_funnel_kpis(df_funnel) if df_funnel is not None else None
    total_revenue = float(df_revenue["TOTAL_REVENUE"].sum()) if not df_revenue.empty else 0.0

    col1, col2, col3, col4 = st.columns(4)

    if kpis:
        col1.metric("Total Views", f"{kpis['total_views']:,.0f}")
        col2.metric("Add to Cart", f"{kpis['total_add']:,.0f}")
        col3.metric("Purchases", f"{kpis['total_purch']:,.0f}")
        col4.metric("Revenue Total", f"R$ {total_revenue:,.2f}")

        col5, col6, col7 = st.columns(3)
        col5.metric("View → Cart", f"{kpis['view_to_cart']*100:,.1f}%")
        col6.metric("Cart → Purchase", f"{kpis['cart_to_purchase']*100:,.1f}%")
        col7.metric("View → Purchase", f"{kpis['view_to_purchase']*100:,.1f}%")

        st.markdown("---")

        # Funil acumulativo
        st.subheader("Funil de Conversão (Acumulado)")

        funnel_df = pd.DataFrame(
            {
                "Etapa": ["Interações", "Intenção", "Conversões"],
                "Valor": [
                    kpis["funnel_interactions"],
                    kpis["funnel_intent"],
                    kpis["funnel_conversions"],
                ],
            }
        )

        fig_funnel = px.bar(
            funnel_df,
            x="Etapa",
            y="Valor",
            text="Valor",
        )
        fig_funnel.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig_funnel.update_layout(yaxis_title=None, xaxis_title=None)

        st.plotly_chart(fig_funnel, use_container_width=True)

    else:
        st.warning("Nenhum dado de funil encontrado na tabela GOLD.FUNNEL_DAILY.")

    st.markdown("---")

    # Pequeno resumo de receita (mesmo com 1 dia já fica ok)
    st.subheader("Receita por dia (snapshot)")

    if not df_revenue.empty:
        df_rev_plot = df_revenue.copy()
        df_rev_plot["EVENT_DATE"] = pd.to_datetime(df_rev_plot["EVENT_DATE"])

        fig_rev = px.bar(
            df_rev_plot,
            x="EVENT_DATE",
            y="TOTAL_REVENUE",
            text="TOTAL_REVENUE",
        )
        fig_rev.update_traces(texttemplate="R$ %{text:,.0f}", textposition="outside")
        fig_rev.update_layout(xaxis_title="Data", yaxis_title="Receita")

        st.plotly_chart(fig_rev, use_container_width=True)
    else:
        st.info("Tabela GOLD.REVENUE_DAILY está vazia.")


# ------------------------------
# Página: Produtos
# ------------------------------
elif page == "Produtos":
    st.title("🧺 Performance de Produtos")

    df_prod = get_product_performance()

    if df_prod.empty:
        st.warning("Nenhum dado em GOLD.PRODUCT_PERFORMANCE.")
    else:
        # ranking por receita
        st.subheader("Top Produtos por Receita")

        fig_prod_rev = px.bar(
            df_prod.head(15),
            x="PRODUCT_ID",
            y="TOTAL_REVENUE",
            text="TOTAL_REVENUE",
        )
        fig_prod_rev.update_traces(texttemplate="R$ %{text:,.0f}", textposition="outside")
        fig_prod_rev.update_layout(xaxis_title="Produto", yaxis_title="Receita")

        st.plotly_chart(fig_prod_rev, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top Produtos por Quantidade Vendida")
            fig_qty = px.bar(
                df_prod.head(15),
                x="PRODUCT_ID",
                y="TOTAL_QTY_SOLD",
                text="TOTAL_QTY_SOLD",
            )
            fig_qty.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig_qty.update_layout(xaxis_title="Produto", yaxis_title="Qtd. Vendida")
            st.plotly_chart(fig_qty, use_container_width=True)

        with col2:
            st.subheader("Taxa de Conversão (View → Purchase)")
            fig_conv = px.bar(
                df_prod.head(15),
                x="PRODUCT_ID",
                y="VIEW_TO_PURCHASE_RATE",
            )
            fig_conv.update_layout(
                xaxis_title="Produto",
                yaxis_title="View → Purchase",
            )
            st.plotly_chart(fig_conv, use_container_width=True)

        st.markdown("---")
        st.subheader("Tabela Completa de Produtos")
        st.dataframe(df_prod)


# ------------------------------
# Página: Campanhas
# ------------------------------
elif page == "Campanhas":
    st.title("📣 Performance de Campanhas")

    df_campaign = get_campaign_performance()

    if df_campaign.empty:
        st.warning("Nenhum dado em GOLD.CAMPAIGN_PERFORMANCE.")
    else:
        st.subheader("Top Campanhas por Receita")

        fig_camp_rev = px.bar(
            df_campaign.head(15),
            x="CAMPAIGN_ID",
            y="TOTAL_REVENUE",
            text="TOTAL_REVENUE",
        )
        fig_camp_rev.update_traces(texttemplate="R$ %{text:,.0f}", textposition="outside")
        fig_camp_rev.update_layout(
            xaxis_title="Campanha",
            yaxis_title="Receita Total",
        )
        st.plotly_chart(fig_camp_rev, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Conversão View → Purchase")
            fig_camp_conv = px.bar(
                df_campaign.head(15),
                x="CAMPAIGN_ID",
                y="VIEW_TO_PURCHASE_RATE",
            )
            fig_camp_conv.update_layout(
                xaxis_title="Campanha",
                yaxis_title="View → Purchase",
            )
            st.plotly_chart(fig_camp_conv, use_container_width=True)

        with col2:
            st.subheader("Ticket Médio por Campanha")
            fig_camp_ticket = px.bar(
                df_campaign.head(15),
                x="CAMPAIGN_ID",
                y="AVG_TICKET",
                text="AVG_TICKET",
            )
            fig_camp_ticket.update_traces(texttemplate="R$ %{text:,.0f}", textposition="outside")
            fig_camp_ticket.update_layout(
                xaxis_title="Campanha",
                yaxis_title="Ticket Médio",
            )
            st.plotly_chart(fig_camp_ticket, use_container_width=True)

        st.markdown("---")
        st.subheader("Tabela Completa de Campanhas")
        st.dataframe(df_campaign)
