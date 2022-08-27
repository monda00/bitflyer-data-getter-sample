import const
import pandas as pd
import requests
import time

from google.cloud import storage as gcs
from io import BytesIO

storage_client = gcs.Client(const.PROJECT_ID)


def save_data(df, bucket_name, filepath):
    '''
    データの保存

    Parameters
    -----------
    df : Dataframe
        保存するDataframe
    bucket_name : string
        保存先のバケット名
    filepath : string
        ファイルのパス
    '''
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filepath)
    blob.upload_from_string(df.to_csv())


def get_data_from_gcs():
    '''
    GCSから過去のデータを取得

    Returns
    -----------
    df : Dataframe
        GCSから取得した過去のデータのDataframe
    '''
    bucket = storage_client.get_bucket(const.DATA_BUCKET_NAME)
    blob = bucket.blob(const.CHART_PATH)
    df = pd.read_csv(BytesIO(blob.download_as_string()), index_col=0)

    return df


def create_new_data(df_recent):
    '''
    新しいデータの作成

    Parameters
    -----------
    df_recent : Dataframe
        直近のデータのDataframe

    Returns
    -----------
    df : Dataframe
        既存データのと新しいデータを合わせたDataframe
    '''
    df_gcs = get_data_from_gcs()
    df = pd.concat([df_gcs, df_recent]).drop_duplicates()

    return df


def get_recent_data():
    '''
    cryptowatchから直近のデータを取得

    Returns
    -----------
    df : Dataframe
        直近のデータのDataframe[op, hi, lo, cl, volume]
    '''
    now = str(int(time.time()))
    params = {"periods": str(const.PERIOD), "before": now}
    columns = ["CloseTime", "op", "hi", "lo", "cl", "volume", "QuoteVolume"]

    res = requests.get(const.BTC_URL, params).json()
    df = pd.DataFrame(res['result'][str(const.PERIOD)], columns=columns)

    df.index = pd.to_datetime(
        df['CloseTime'], unit='s', utc=True).dt.tz_convert('Asia/Tokyo')

    df.index.name = 'timestamp'
    df = df.drop(['CloseTime', 'QuoteVolume'], axis=1)

    return df
