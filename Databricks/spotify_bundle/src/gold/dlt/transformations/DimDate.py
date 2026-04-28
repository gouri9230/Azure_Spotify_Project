import dlt

@dlt.table
def dimdate_stage():
    df = spark.readStream.table("spotify_catalog.silver.dimdate")
    return df

dlt.create_streaming_table("dimdate")

#creates SCD automatically using auto cdc flow
dlt.create_auto_cdc_flow(
    target = "dimdate",
    source = "dimdate_stage",
    keys = ["date_key"],
    sequence_by = "date",
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False
)