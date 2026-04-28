import dlt

@dlt.table
def factstream_stage():
    df = spark.readStream.table("spotify_catalog.silver.factstream")
    return df

dlt.create_streaming_table("factstream")

#creates SCD automatically using auto cdc flow
dlt.create_auto_cdc_flow(
    target = "factstream",
    source = "factstream_stage",
    keys = ["stream_id"],
    sequence_by = "stream_timestamp",
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False
)