import os
import shutil
import pandas as pd
import datetime
import urllib
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import threading
import logging
from logging.handlers import RotatingFileHandler

class ReadAndWriteToDatabase:

    def __init__(self):
        logger.info("Process Started at : " + str(datetime.datetime.now()))

    def lookup(self,s):
        dates = {date: pd.to_datetime(date) for date in s.unique()}
        return s.map(dates)

    def read_from_csv(self):
        try:
            path='path_to_local_folder'
            columns = [columns of data in CSV]
            dtypes = {'cost': 'int64', 'name': str, 'date': str, 'author_name': str,'cost_in_dollars': float}
            for filename in os.listdir(path):
                print(filename)
                logger.info("Reading Started ::"+str(datetime.datetime.now()))
                data = pd.read_csv(path + '/' + filename, low_memory=True, dtype=dtypes)
                data['ts_local'] = self.lookup(data['ts_local'])
                data['ts_utc'] = self.lookup(data['ts_utc'])
                t1 = threading.Thread(target=self.write_to_database, args=(data,))
                t1.start()
                logger.info("Writing into DataBase ::" + str(datetime.datetime.now()))
            shutil.rmtree(path)
        except:
            logger.info("Error while Reading the file")

    def write_to_database(self,data):
        try:
            params = urllib.parse.quote_plus(
                "DRIVER={SQL Server};SERVER=xxxxxxx;DATABASE=xxxxx;UID=xxxx;PWD=xxxxx")
            Session = sessionmaker()
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, isolation_level='AUTOCOMMIT',
                                   fast_executemany=True)
            Session.configure(bind=engine, autocommit=True)
            session = Session()
            session.begin()
            data.to_sql('test_table', con=engine, schema='xxxxx.xxxx', index=False, if_exists='append',
                        chunksize=10000
                        , dtype={'cost': sqlalchemy.types.INTEGER(),
                                 'date': sqlalchemy.DateTime(), 'name': sqlalchemy.types.String(),
                                 'author_name':sqlalchemy.types.String(),
                                 'cost_in_dollars': sqlalchemy.types.Float(precision=6, asdecimal=True)})
            session.commit()
            logger.info("Data is Saved in DataBase ::" + str(datetime.datetime.now()))
        except:
            logger.info("Error while writing to Database")
if __name__ == '__main__':
    logger = logging.getLogger('ReadAndWriteToDatabase')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler('newlogger.log', maxBytes=5000000, backupCount=10)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(funcName)s :  %(message)s ')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    rd = ReadAndWriteToDatabase()
    rd.read_from_csv()
