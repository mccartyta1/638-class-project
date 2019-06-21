from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests
import re

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# TODO: Improve prefix/suffix calibration to be more automatic, possibly pickle output
def findNewPrefixSuffix(stockValue):
    """
    I used this to determinethe prefix and suffix for web scraping.
    If the matcher is finding no content, use this function to find the new prefix and suffix.
    """
    url = 'https://finance.yahoo.com/quote/DIS/'
    request = requests.get(url)
    pageContent = request.text
    startIndex = pageContent.index(str(stockValue))
    print("Index found: ", startIndex)
    print("Recommendation for New Prefix: ", pageContent[startIndex - 20: startIndex - 1] + "|")
    endIndex = startIndex + len(str(stockValue))
    print("Recommendation for New Suffix: ", pageContent[endIndex: endIndex + 10])

def checkValue(psDict, skip_same_values=True, sleep_duration=100):
    """
    Program loop to periodically check the value scraped from Google's results for a search
    on the url. Based on a prefix and suffix passed in, the value will be pushed to Kafka then
    the loop will sleep as to not constantly query Google.

    Duration for sleep is set by sleep_duration.

    Whether to push the same value is set by skip_same_values, as the values will not change
        after the market closes.

    The Prefix and Suffix are used by a Regex string to parse the returned request, and return
    the desired value that lies in between them. For Google, it tends to be an odd class name
    towards the end of a span tag, the value, and then closing that span tag follow by an odd div class name.

    While creating this, I have noticed Google seems to block you after too many requests, so be wary
    of needing to restart this. If this happens often, maybe try updating the sleep_duration.
    """
    old_value = None
    url = 'https://finance.yahoo.com/quote/DIS/'
    first_run = True
    while True:
        # If not the first run, wait until making the request.
        if not first_run:
            sleep(sleep_duration)
        else:
            first_run = False
        # Make request
        print("Checking value of stock at url: ", url)
        request = requests.get(url)
        content = request.text

        # Scrape values desired using supplied prefixes and suffixes
        close = getValueWithPrefixAndSuffix(psDict["closePrefix"], psDict["closeSuffix"], content, 0)
        open = getValueWithPrefixAndSuffix(psDict["openPrefix"], psDict["openSuffix"], content, close)
        low = getValueWithPrefixAndSuffix(psDict["lowPrefix"], psDict["lowSuffix"], content, close)
        high = getValueWithPrefixAndSuffix(psDict["highPrefix"], psDict["highSuffix"], content, close)
        volume = getValueWithPrefixAndSuffix(psDict["volumePrefix"], psDict["volumeSuffix"], content, 13000000)

        # If the close is the same value, skip instead of repushing to Kafka
        if skip_same_values and old_value is not None and old_value == close:
            print("Value has not changed. Sleeping.")
            continue
        old_value = close

        # Attempt to push to Kafka. Using the numtest topic from the example to reduce Kafka's
        # temper with Windows and not unleash its wrath
        try:
            data = {"adjusted_close": close, "volume": volume, "high": high, "low": low, "close": close, "open": open}
            print("Sending data: ", data, ". Sleeping.")
            producer.send('numtest', value=data)
        except:
            print("Prefixes and Suffixes are not set correctly. Exiting.")
            break

def getValueWithPrefixAndSuffix(prefix, suffix, content, default):
    """
    Utility method that uses a Regex that refuses spaces or dashes to try and find numerical values.
    Utilizes a preset prefix and suffix to find the values.
    Most of these lines are to log issues, as this required several hours of tweaking.
    Default is offered just in case.
    """
    matches = re.search(re.escape(prefix) + '([^\s-]*)' + re.escape(suffix), content)

    # Handle if nothing was matched and log the issue.
    if matches is None:
        if prefix not in content:
            print("Didn't have that prefix...")
        if suffix not in content:
            print("Didn't have that suffix...")
        print("Prefixes and Suffixes are not set correctly. Exiting.")
        return default

    # Extract the value and attempt to get the float value of it
    value = matches.group(1).split(">")[-1].replace(",", "")
    try:
        return float(value)
    except:
        return default

if __name__ == "__main__":
    # The following values may change, the danger of webscraping
    # If they do, use the findNewPrefixSuffix method
    psDict = {}
    psDict["closePrefix"] = 'ctid="14">'
    psDict["closeSuffix"] = '</span><div class="D(ib) Va(t) W(4'
    psDict["highPrefix"] = 'actid="35">139.91 - '
    psDict["highSuffix"] = "</td></tr>"
    psDict["lowPrefix"] = '" data-reactid="35"'
    psDict["lowSuffix"] = ' - 142.23<'
    psDict["openPrefix"] = '" data-reactid="21"'
    psDict["openSuffix"] = '</span></t'
    psDict["volumePrefix"] = '" data-reactid="49"'
    psDict["volumeSuffix"] = '</span></t'
    checkValue(psDict)

    """
    Example usage of finding prefixes and suffixes:
    print("volume")
    findNewPrefixSuffix("13,074,471")
    print("high")
    findNewPrefixSuffix(142.23)
    print("low")
    findNewPrefixSuffix(139.91)
    """
