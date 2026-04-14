from sqlalchemy import create_engine, Numeric
from datetime import date
from dbmerge import dbmerge


engine = create_engine("yql+ydb://localhost:2136/local")

data = [
    {'Shop': '123', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 2, 'Price': 50.10},
    {'Shop': '124', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 1, 'Price': 100.50}
]

# # 3. Execute the merge operation
# # The table will be created automatically if it doesn't exist.
with dbmerge(engine=engine, data=data, table_name="Facts", 
             key=['Shop', 'Product', 'Date'], data_types={'Price':Numeric(10,3)}) as merge:
    merge.exec()
