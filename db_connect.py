import sqlalchemy

def get_connection(cfg):
    uri = f"postgresql+psycopg2://{cfg['db_user']}:{cfg['db_pass']}@{cfg['db_host']}:{cfg['db_port']}/{cfg['db_name']}"
    engine = sqlalchemy.create_engine(uri)
    return engine
