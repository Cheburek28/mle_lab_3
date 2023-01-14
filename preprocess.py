import os
import pandas as pd
import sys
import traceback
from sqlalchemy import create_engine
import pymysql


from logger import Logger

SHOW_LOG = True


class DataMaker:
    """
        Class for preparing data which will be used for training and predicting
    """
    def __init__(self) -> None:
        """
            Re-defined __init__ method which sets predicting model
        Args:
            log - logger object which uses for logging events
            config - data readed from config.ini which consist of paths to datasets and saved model
            X_train, X_test, y_pred, y_test - datasets of features and results
        """
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)
        self.project_path = os.path.join(os.getcwd(), "data")
        self.data_path = os.path.join(self.project_path, "en.openfoodfacts.org.products.csv")
        self.log.info("DataMaker is ready")

    def proceed_data(self):
        df = pd.read_csv(self.data_path, sep="	", nrows=30000, low_memory=False)

        nonmodel_columns = "code,url,creator,created_t,created_datetime,last_modified_t,last_modified_datetime," \
                           "product_name," \
                  "abbreviated_product_name,generic_name,quantity,packaging,packaging_tags,packaging_en," \
                  "packaging_text,brands,brands_tags,categories,categories_tags,categories_en,origins," \
                  "origins_tags,origins_en,manufacturing_places,manufacturing_places_tags,labels,labels_tags," \
                  "labels_en,emb_codes,emb_codes_tags,first_packaging_code_geo,cities,cities_tags," \
                  "purchase_places,stores,countries,countries_tags,countries_en,ingredients_text," \
                  "ingredients_tags,ingredients_analysis_tags,allergens,allergens_en,traces,traces_tags,"\
                  "traces_en,serving_size,serving_quantity,no_nutriments,additives_n,additives,additives_tags," \
                  "additives_en,nutriscore_score,nutriscore_grade,nova_group,pnns_groups_1,pnns_groups_2," \
                  "food_groups,food_groups_tags,food_groups_en,states,states_tags,states_en,brand_owner," \
                  "ecoscore_score,ecoscore_grade,nutrient_levels_tags,product_quantity,owner," \
                  "data_quality_errors_tags,unique_scans_n,popularity_tags,completeness,last_image_t," \
                  "last_image_datetime,main_category,main_category_en,image_url,image_small_url," \
                  "image_ingredients_url,image_ingredients_small_url,image_nutrition_url,image_nutrition_small_url"

        nonmodel_columns = nonmodel_columns.split(",")

        for col in nonmodel_columns:
            del df[col]

        new_coll_names_d = {}

        for col in list(df.keys()):
            new_coll_names_d.update({col: col.replace("-", "_")})

        df.rename(new_coll_names_d, axis=1, inplace=True)

        df = df.fillna(value=0)

        for col in df.columns:
            pd.to_numeric(df[col])

        sqlEngine = create_engine('mysql+pymysql://artem:artem@127.0.0.1:6603/MLE_LAB_3', pool_recycle=3600)
        dbConnection = sqlEngine.connect()

        try:
            df.to_sql("data", dbConnection)
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

