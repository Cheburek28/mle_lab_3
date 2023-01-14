from preprocess import DataMaker
import pandas as pd
from sqlalchemy import create_engine
import sys
import traceback

from logger import Logger

SHOW_LOG = True


class DataMart:
    """Class which gets data from storage and puts it into it"""
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.sqlEngine = create_engine('mysql+pymysql://artem:artem@127.0.0.1:6603/MLE_LAB_3', pool_recycle=3600)

    def get_unclassified_data(self) -> pd.DataFrame:
        conn = self.sqlEngine.connect()
        df = pd.read_sql("SELECT * FROM data;", conn)
        del df['index']
        conn.close()
        return df

    def set_classified_data(self, data: pd.DataFrame):
        conn = self.sqlEngine.connect()
        try:
            data.to_sql("data_classified", conn, if_exists="replace")
        except ValueError as e:
            self.log.error(e)
            sys.exit(1)
        except Exception:
            self.log.error(traceback.format_exc())
            sys.exit(1)
        else:
            self.log.info("Classisied data added to database successfully")
        finally:
            conn.close()

        conn.close()

    def get_classified_data(self):
        pass

    @staticmethod
    def update_unclassified_data():
        DataMaker().proceed_data()
