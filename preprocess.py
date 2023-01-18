import os
import pandas as pd
import sys
import traceback
from sqlalchemy import create_engine
import configparser


from logger import Logger

SHOW_LOG = True


class DataMaker:
    """
        Class for preparing data which will be used for classifying
    """
    def __init__(self) -> None:
        logger = Logger(SHOW_LOG)
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")

        self.log = logger.get_logger(__name__)
        self.project_path = os.path.join(os.getcwd(), "data")
        self.data_path = os.path.join(self.project_path, "en.openfoodfacts.org.products.csv")
        self.log.info("DataMaker is ready")

    def proceed_data(self):
        import random

        n = sum(1 for line in open(self.data_path)) - 1  # number of records in file
        s = 100000  # desired sample size

        skip = sorted(random.sample(range(1, n+1), n-s))

        df = pd.read_csv(self.data_path, sep="	", skiprows=skip, low_memory=False)

        df = df[['energy_100g', 'fat_100g', 'saturated-fat_100g',
       'monounsaturated-fat_100g', 'polyunsaturated-fat_100g',
       'trans-fat_100g', 'cholesterol_100g', 'carbohydrates_100g',
       'sugars_100g', 'fiber_100g', 'proteins_100g', 'salt_100g',
       'sodium_100g', 'alcohol_100g', 'vitamin-a_100g', 'vitamin-d_100g',
       'vitamin-e_100g', 'vitamin-c_100g', 'vitamin-b1_100g',
       'vitamin-b2_100g', 'vitamin-pp_100g', 'vitamin-b6_100g',
       'vitamin-b9_100g', 'folates_100g', 'vitamin-b12_100g',
       'pantothenic-acid_100g', 'potassium_100g', 'calcium_100g',
       'phosphorus_100g', 'iron_100g', 'magnesium_100g', 'zinc_100g',
       'copper_100g', 'manganese_100g', 'selenium_100g',
       'fruits-vegetables-nuts_100g', 'cocoa_100g']]

        new_coll_names_d = {}

        for col in list(df.keys()):
            new_coll_names_d.update({col: col.replace("-", "_")})

        df.rename(new_coll_names_d, axis=1, inplace=True)

        df = df.fillna(value=0)

        for col in df.columns:
            pd.to_numeric(df[col])

        host = self.config["DATABASE_AUTHORIZATION"]["host"]
        port = self.config["DATABASE_AUTHORIZATION"]["port"]
        database = self.config["DATABASE_AUTHORIZATION"]["database"]
        password = self.config["DATABASE_AUTHORIZATION"]["password"]
        login = self.config["DATABASE_AUTHORIZATION"]["login"]

        sqlEngine = create_engine(f'mysql+pymysql://{login}:{password}@{host}:{port}/{database}', pool_recycle=3600)
        dbConnection = sqlEngine.connect()

        try:
            df.to_sql("data", dbConnection, if_exists="replace")
        except ValueError as e:
            self.log.error(e)
            sys.exit(1)
        except Exception:
            self.log.error(traceback.format_exc())
            sys.exit(1)
        else:
            self.log.info("Data added to database successfully")
        finally:
            dbConnection.close()

        print(df)


if __name__ == "__main__":
    dm = DataMaker()
    dm.proceed_data()

