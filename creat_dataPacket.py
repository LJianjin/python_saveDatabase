# -*- coding: utf-8 -*-
# from ftplib import FTP
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# import schedule
# import time
# import datetime
import logging
# import re
# import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def run():
    logging.info('in creat database function')

    engine = create_engine('postgresql://postgres:Supwin999@#$@sg.gaawei.com:45432')
    connection = engine.raw_connection()

    exists = engine.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'test_forex'")
    print(exists.rowcount)
    if not exists.rowcount:
        session = sessionmaker(bind=engine)()
        session.connection().connection.set_isolation_level(0)
        session.execute("CREATE DATABASE test_forex ENCODING 'UTF8'")
        session.connection().connection.set_isolation_level(1)
        engineTime = create_engine('postgresql://postgres:Supwin999@#$@sg.gaawei.com:45432/test_forex')
        # connectionll = engineTime.raw_connection()

        engineTime.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
    logging.info('database is creat: {}')
  

if __name__ == "__main__":
    run()

