import polars as pl

orders_df = pl.DataFrame(
    [
        {"order_id": 1, "customer_id": 1, "order_date": "2021-01-01", "total": 100.0},
        {"order_id": 2, "customer_id": 1, "order_date": "2021-01-02", "total": 200.0},
        {"order_id": 3, "customer_id": 2, "order_date": "2021-01-03", "total": 300.0},
        {"order_id": 4, "customer_id": 2, "order_date": "2021-01-04", "total": 400.0},
        {"order_id": 5, "customer_id": 3, "order_date": "2021-01-05", "total": 500.0},
        {"order_id": 6, "customer_id": 1, "order_date": "2021-01-06", "total": 600.0},
    ]
)

customers_df = pl.DataFrame(
    [
        {"customer_id": 1, "customer_name": "Alice"},
        {"customer_id": 2, "customer_name": "Bob"},
        {"customer_id": 3, "customer_name": "Calvin"},
        {"customer_id": 4, "customer_name": "Dana"},
    ]
)