from config import run_query


def get_funnel_daily():
    return run_query("SELECT * FROM ECOMMERCE.GOLD.FUNNEL_DAILY")


def get_revenue_daily():
    return run_query("SELECT * FROM ECOMMERCE.GOLD.REVENUE_DAILY")


def get_product_performance():
    return run_query(
        """
        SELECT *
        FROM ECOMMERCE.GOLD.PRODUCT_PERFORMANCE
        ORDER BY TOTAL_REVENUE DESC
        """
    )


def get_campaign_performance():
    return run_query(
        """
        SELECT *
        FROM ECOMMERCE.GOLD.CAMPAIGN_PERFORMANCE
        ORDER BY TOTAL_REVENUE DESC
        """
    )


def compute_funnel_kpis(df_funnel):
    """Recebe FUNNEL_DAILY e devolve dict com KPIs e funil acumulado."""
    if df_funnel.empty:
        return None

    total_views = float(df_funnel["VIEWS"].sum())
    total_add = float(df_funnel["ADD_TO_CART"].sum())
    total_purch = float(df_funnel["PURCHASES"].sum())

    # funil acumulativo pra evitar incoerência (purchases > add_to_cart etc.)
    interactions = total_views + total_add + total_purch
    intent = total_add + total_purch
    conversions = total_purch

    # taxas (protege divisão por zero)
    view_to_cart = (total_add / total_views) if total_views > 0 else 0
    cart_to_purchase = (total_purch / total_add) if total_add > 0 else 0
    view_to_purchase = (total_purch / total_views) if total_views > 0 else 0

    return {
        "total_views": total_views,
        "total_add": total_add,
        "total_purch": total_purch,
        "funnel_interactions": interactions,
        "funnel_intent": intent,
        "funnel_conversions": conversions,
        "view_to_cart": view_to_cart,
        "cart_to_purchase": cart_to_purchase,
        "view_to_purchase": view_to_purchase,
    }
