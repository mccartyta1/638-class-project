import pandas
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn import preprocessing, svm
from sklearn.model_selection import train_test_split
from kafka import KafkaConsumer
from json import loads
from sklearn.preprocessing import StandardScaler

# Referenced the following:
# https://towardsdatascience.com/in-12-minutes-stocks-analysis-with-pandas-and-scikit-learn-a8d8a7b50ee7
# https://towardsdatascience.com/predicting-stock-prices-with-python-ec1d0c9bece1

## Link for obtaining DIS stock history:
# https://query1.finance.yahoo.com/v7/finance/download/DIS?period1=-252356400&period2=1560657600&interval=1d&events=history&crumb=Cd0ZdeBNKlZ
# Periods are in Unix millis, second period would be current time.
# TODO: More of a nice-to-have would be fetching the csv if the current one is out of date.
# TODO: Redo data organization for yValue to be true or false

def live_model(update_period, csv_path):
    """
    Method checks consumes from Kafka every update_period and will predict
    based on new input.
    Referenced this for using Consumer https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1
    Something about the settings passed to Consumer causes Kafka to fatally error, so keeping Consumers simple.
    If on Windows, if Kafka fails, both Kafka and Zookeeper will need its logs emptied to restart.
    """
    consumer = KafkaConsumer('numtest', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest' )
    scaler = StandardScaler()

    # init model
    # TODO: If updating csv functionality added, pickle model here to reduce start up
    # times where this is restarted during the same day
    model, scaler = make_model(csv_path)
    print("start reading")
    for message in consumer:
        loaded_message = loads(message.value)
        print("Loaded message: ", message.value)
        try:
            adj_close = loaded_message["adjusted_close"]
            volume = loaded_message["volume"]
            high = loaded_message["high"]
            low = loaded_message["low"]
            close = loaded_message["close"]
            open = loaded_message["open"]

            hlp = get_hlp(high, low, close)
            change = get_change(close, open)
            df2 = pandas.DataFrame([[adj_close, volume, hlp, change]], columns=["Adj Close", "Volume", "hlp", "change"])
            scaled_df2 = scaler.transform(df2)
            print("Predicting with given data from Kafka stream: ", df2)
            print(model.predict(np.array(scaled_df2)))
        except:
            print("Error: Data pushed to Kafka topic numtest does not conform.")

def get_hlp(high, low, close):
    # Makes High Low Percentage
    return (high - low) / close * 100.0

def get_change(close, open):
    # Gets the Percent change
    return (close - open) / open * 100.0

def make_model(csv_path):
    """
    Creates the model using LinearRegression off of the supplied CSV path.
    Similar to test_on_csv, except it does not exclude the last day.
    """
    days_to_predict = 1
    df = pandas.read_csv(csv_path)
    dfreg = df.loc[:, ["Adj Close","Volume"]]
    dfreg["hlp"] = (df["High"] - df["Low"]) / df["Close"] * 100.0
    dfreg["change"] = (df["Close"] - df["Open"]) / df["Open"] * 100.0
    dfreg["yValue"] = df["Adj Close"]
    dfreg.dropna(inplace=True)

    X = np.array(dfreg.drop(['yValue'], 1))
    Y = np.array(dfreg['yValue'])
    scaler = StandardScaler(copy=True, with_mean=True, with_std=True)
    scaler.fit(X)
    X = scaler.transform(X)

    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size = 0.6)

    model = LinearRegression()
    model.fit(X_train, Y_train)
    return model, scaler

def test_on_csv(csv_path, days_to_predict):
    """
    Almost the same as making the model, except drops a value to predict on.
    """

    # Set up data frame from the csv
    df = pandas.read_csv(csv_path)
    dfreg = df.loc[:, ["Adj Close","Volume"]]
    dfreg["hlp"] = (df["High"] - df["Low"]) / df["Close"] * 100.0
    dfreg["change"] = (df["Close"] - df["Open"]) / df["Open"] * 100.0
    dfreg["yValue"] = df["Adj Close"].shift(-days_to_predict)
    dfreg.dropna(inplace=True)

    # X is the independent variable or frame of features
    X = np.array(dfreg.drop(['yValue'], 1))
    # Y is the predicted value, in this case the adjusted close
    Y = np.array(dfreg['yValue'])
    # We're scaling X so large values don't throw everything off (volume) and then taking off the last day to predict.
    X = preprocessing.scale(X)
    X_prediction = X[-days_to_predict:]

    # Make a train and test split, we're not playing too much with the model here,
    # but the goal is to test and figure out ideal feature sets and models
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size = 0.8)

    model = LinearRegression()
    model.fit(X_train, Y_train)
    # After creating the model, print out result:
    result = model.predict(X_prediction)
    actual = df["Adj Close"].iloc[-1]
    print("Predicted Close: ", result)
    print("Actual Close: ", actual)
    print((((result[0] - actual) / actual) * 100), "%")

if __name__ == "__main__":
    # CSV should be included at local path, just change the path here if issues.
    # TODO: Use argparse for path
    live_model(1, "DIS.csv")
