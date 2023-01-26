from flask import Flask, request, jsonify, make_response
import threading
import pandas as pd
import os
import time
import sched
import time
from io import BytesIO
import gzip


s = sched.scheduler(time.time, time.sleep)

app = Flask(__name__)

# Create a dictionary to store the DataFrames, with the keys being the symbol names
dataframes = {}


def load_dataframe(symbol):
    if symbol not in dataframes:
        file_path = f'{symbol}.pkl'
        if os.path.exists(file_path):
            dataframes[symbol] = pd.read_pickle(file_path)
        else:
            dataframes[symbol] = pd.DataFrame(columns=['price'],index = pd.DatetimeIndex([]))
        dataframes[symbol].dirty = False
    return dataframes[symbol]

1
def batch_insert(df, data):
    timestamps = [item['timestamp'] for item in data['items']]
    prices = [item['price'] for item in data['items']]
    df.loc[timestamps, 'price'] = prices
    df = df.sort_index()
    return df


@app.route('/get_data', methods=['GET'])
def get_data():
    symbol = request.args.get('symbol')
    start_timestamp = pd.to_datetime(request.args.get('start_timestamp'))
    end_timestamp = pd.to_datetime(request.args.get('end_timestamp'))
    df = load_dataframe(symbol)
    df = df.loc[start_timestamp:end_timestamp]

    # Compress the DataFrame
    bio = BytesIO()
    with gzip.open(bio, 'wb') as f:
        df.to_pickle(f)
    compressed_data = bio.getvalue()

    # Return the compressed data as binary
    response = make_response(compressed_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    response.headers.set('Content-Disposition', 'attachment', filename=f'{symbol}.gz')
    return response


@app.route('/batch_append', methods=['POST'])
def batch_append():
    data = request.json
    symbol = data['symbol']
    df = load_dataframe(symbol)
    for item in data['items']:
        timestamp = item['timestamp']
        price = item['price']
        df.at[timestamp, 'price'] = price
    return jsonify(success=True)


@app.route('/append', methods=['POST'])
def append_data():
    # Get the symbol, timestamp, and price from the request
    symbol = request.json['symbol']
    timestamp = request.json['timestamp']
    price = request.json['price']

    # Append the data to the appropriate DataFrame
    df = load_dataframe(symbol)
    df.at[timestamp, 'price'] = price
    df.dirty = True
    dataframes[symbol] = df

    return jsonify(success=True)


def flush_dataframes(sc):
    for symbol, df in dataframes.items():
        if df.dirty:
            file_path = f'{symbol}.pkl'
            df.to_pickle(file_path)
    sc.enter(60, 1, flush_dataframes, (sc,))


s = sched.scheduler(time.time, time.sleep)
s.enter(60, 1, flush_dataframes, (s,))
s.run()


if __name__ == '__main__':
    app.run()
