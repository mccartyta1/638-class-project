# Stock Prediction Project

This a project done for CSIS638 that utilizes Pandas, Scikit Learn, Requests, and Apache Kafka.

# Dependencies

Python 3.7
kafka-python 1.4.6
pandas 0.24.2
scikit-learn 0.21.2
requests 2.22.0

## Kafka

Apache Kafka must be running for both producing and consuming to occur.
Keep in mind that includes installing and running Zookeeper before it as well.
See [here](https://kafka.apache.org/quickstart), or [here for Windows which I used.](https://medium.com/@shaaslam/installing-apache-kafka-on-windows-495f6f2fd3c8)

# Running

Run both files as the same time to observe the Kafka Host producing JSON content that the Predictor consumes.

Included is the DIS.csv, which is likely out of date.
The model has not been tweaked much towards accuracy so it should not be an issue.

You could swap out the CSV if you also change the currently hardcoded URLs, and likely recalibrate the prefixes and suffixes.
