import dlt

@dlt.table
def dimtrack_stage():
    df = spark.readStream.table("spotify_catalog.silver.dimtrack")
    return df

dlt.create_streaming_table("dimtrack")

#creates SCD automatically using auto cdc flow
dlt.create_auto_cdc_flow(
    target = "dimtrack",
    source = "dimtrack_stage",
    keys = ["track_id"],
    sequence_by = "updated_at",
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False
)