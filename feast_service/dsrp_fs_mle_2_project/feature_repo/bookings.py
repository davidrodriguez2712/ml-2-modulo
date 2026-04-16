from feast import FeatureStore
from feast import (
    Entity,
    FeatureView,
    FileSource,
    Field,
    RequestSource,
    FeatureService,
    PushSource
    )
from feast.types import Int64, Float64, String
from feast.on_demand_feature_view import on_demand_feature_view
import pandas as pd

import numpy as np
import uuid
import time
from datetime import datetime
from sklearn.preprocessing import LabelEncoder,OrdinalEncoder, OneHotEncoder
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import VarianceThreshold
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
from loguru import logger

class FeatureEngineeringProcessor:
    def __init__(self, raw_data: pd.DataFrame, pipeline_name: str) -> None:
        self.raw_data = raw_data
        self.pipeline_name = pipeline_name
        self.feature_table = None

    def impute_scale(self) -> pd.DataFrame:
        """Pipeline que imputa variables numérica y luego las escala, para
        finalmente aplicar PCA y quedarse con N componentes principales"""
        numeric_cols = [
            "lead_time",
            "adults",
            "children",
            "babies",
            "adr"
        ]
        pipe = Pipeline(
            steps = [
                ("imputer_mean", SimpleImputer(strategy= "mean")),
                ("std_scaling", StandardScaler()),
                ("pca", PCA(n_components = 2))
            ]
        )
        return pd.DataFrame(
            pipe.fit_transform(self.raw_data[numeric_cols]),
            columns = ["great_feature1", "great_feature2"]
        )
    
    
    def encode_categoricals(self) -> pd.DataFrame:

        encoded_vars = []
        for var in ["hotel", "market_segment", "reserved_room_type"]: 
            encoder = OneHotEncoder()
            logger.info(f"Codificando con OHE {var}")
            encoded = encoder.fit_transform(self.raw_data[[var]]).toarray()
            cols = [f"{var}_{col}" for col in encoder.categories_[0]]
            _dataframe = pd.DataFrame(
                encoded,
                columns= cols
            )
            encoded_vars.append(_dataframe)
        return pd.concat(encoded_vars, axis= 1)
                                  
        
    def run(self) -> pd.DataFrame:
        # acá podrenmos nuestro código
        logger.info(f"Inicializando pipeline {self.pipeline_name}")

        categorical = self.encode_categoricals()
        numerics = self.impute_scale()

        modeling_dataset = pd.concat([categorical, numerics], axis = 1)
        
        # Dataset Previo al pipeline
        pipe = Pipeline(
            steps = [
                ("feature_selection", VarianceThreshold()),
                ("scaling_robust", RobustScaler())
            ]
        )
        self.feature_table = pd.DataFrame(
            pipe.fit_transform(modeling_dataset),
            columns = modeling_dataset.columns
        )
        self.feature_table["booking_id"] = [str(uuid.uuid4()) for _ in range(self.feature_table.shape[0])]
        self.feature_table["event_timestamp"] = [datetime.now() for _ in range(self.feature_table.shape[0])]
        time.sleep(1)
        self.feature_table["created"] = [datetime.now() for _ in range(self.feature_table.shape[0])]
        self.feature_table["event_timestamp"] = pd.to_datetime(self.feature_table["event_timestamp"], utc=True)
        self.feature_table["created"] = pd.to_datetime(self.feature_table["created"], utc=True)
        
        return self.feature_table

    def write_feature_table(self, filepath: str) -> None:
        """Escribimos la feature table final para modelamiento"""
        if self.feature_table is not None:
            self.feature_table.to_parquet(filepath, index = False)
        else:
            raise Exception("La feature table no ha sido creada")


    

booking = Entity(name = "booking", join_keys = ["booking_id"])

## Recuerda el flujo lógico de feast:
## PushSource -> FeatureView -> OnDemandFeatureView -> FeatureService



booking_source = FileSource( 
    name= "booking_source",
    path = "./data/bookings_feature_table.parquet",
    timestamp_field= "event_timestamp",
    created_timestamp_column= "created"
)

booking_push_source = PushSource(
    name = "booking_push_source",
    batch_source = booking_source
)

pc_booking_view = FeatureView(
    name= "pc_booking_view",
    entities= [booking],
    online = True,
    schema= [
        Field(name= "great_feature1", dtype= Float64, description= "Primera variable del PCA"),
        Field(name= "great_feature2", dtype= Float64, description= "Segunda variable del PCA"),
    ],
    source= booking_push_source
)

## El RequestSource es lo que debería recibir de parte del usuario al servidor de feast
input_request = RequestSource(
    name = "input_request",
    schema = [
        Field(name = "kpi1", dtype = Float64),
        Field(name = "kpi2", dtype = Float64)
    ]
)

## En sources del on_demand_feature_view, Feast esperaba una lista de objetos de este tipo:
### FeatureView
### FeatureViewProjection
### RequestSource
@on_demand_feature_view(
    sources = [pc_booking_view, input_request],
    schema = [
        Field(name = "great_feature1_kpi1", dtype = Float64),
        Field(name = "great_feature2_kpi2", dtype = Float64)
    ]
)

def great_feature_view(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["great_feature1_kpi1"] = inputs["great_feature1"] * inputs["kpi1"]
    df["great_feature2_kpi2"] = inputs["great_feature2"] * inputs["kpi2"]
    return df

dsrp_feature_service = FeatureService(
    name = "dsrp_feature_service",
    features = [pc_booking_view, great_feature_view]
)

fs_service_pc = FeatureService(
    name = "fs_service_pc",
    features = [pc_booking_view]
)






