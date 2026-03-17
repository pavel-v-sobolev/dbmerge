from sqlalchemy import create_engine
from datetime import date
from dbmerge import dbmerge

# 1. Initialize DB engine
engine = create_engine("sqlite://")

# 2. Prepare your source data
data = [
    {'Shop': '123', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 2, 'Price': 50.10},
    {'Shop': '124', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 1, 'Price': 100.50}
]

# 3. Execute the merge operation
# The table will be created automatically if it doesn't exist.
with dbmerge(engine=engine, data=data, table_name="Facts", 
             key=['Shop', 'Product', 'Date']) as merge:
    merge.exec()