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






