# run from project folder to initialize module in test mode
# Older versions
rm -rf .venv 
uv venv --python python3.10
uv pip install numpy==1.25
uv pip install pandas==2.0.1
uv pip install sqlalchemy==2.0.23
uv pip install alembic==1.13
uv pip install psycopg2==2.9
uv pip install mariadb==1.1.5
uv pip install pytest==7.1.1
uv pip install -e .

