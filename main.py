from bitflyer_data_getter import *
import const


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    df_recent = get_recent_data()
    df = create_new_data(df_recent)
    save_data(df, const.DATA_BUCKET_NAME, const.CHART_PATH)
