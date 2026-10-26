"""Small pure-Python tests for core retail KPI definitions."""
def calculate_kpis(quantity, unit_price, unit_cost, discount_pct):
    gross_sales = quantity * unit_price
    net_sales = round(gross_sales * (1 - discount_pct), 2)
    total_cost = round(quantity * unit_cost, 2)
    profit = round(net_sales - total_cost, 2)
    margin = round(profit / net_sales * 100, 2) if net_sales else None
    return gross_sales, net_sales, total_cost, profit, margin

def test_basic_profit():
    assert calculate_kpis(2, 100, 60, 0.10) == (200, 180.0, 120, 60.0, 33.33)

def test_full_discount_margin_is_none():
    assert calculate_kpis(1, 100, 10, 1.0)[4] is None
