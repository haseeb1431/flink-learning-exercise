# A real-time data pipeline using Apache Flink

## Intro
The process of the data pipeline involves taking in data from Binance web socket API, modifying it based on specific
business needs, such as decaying sum calculations, and ultimately storing the output data back to the file system. 
It is going to be a real-time pipeline using Apache Filnk that process data continuously as it reads from the source 

For this particular assignment, we are developing a real-time pipeline that sources its data from Binance1, a cryptocurrency exchange. 
The pipeline will apply aggregations to the data and store the resulting data in small batches in a file system. 
The subsequent sections outline the implementation details for each stage of the data pipeline.


### configurations
It reads the setting from the config.yaml file where following settings are required

```yaml
output_path: "output"
symbol: "btcusdt"
half_life: !!java.lang.Long 2000
sample_period: !!java.lang.Long 1000
checkPoint_interval: !!java.lang.Long 30000
```

### Data Input
This program does use the Biannce API to read the data. We use the web socket streams to get the data from the Binance.
"Individual symbol book ticker" streams for the USD futures market will be utilized in our pipeline. This streams
are publicly accessible. The book ticker streams contain data items with a specific structure. The data is received
in the following format

```json
{
  "e":"bookTicker",         // event type
  "u":400900217,            // order book updateId
  "E": 1568014460893,       // event time
  "T": 1568014460891,       // transaction time
  "s":"BNBUSDT",            // symbol
  "b":"25.35190000",        // best bid price
  "B":"31.21000000",        // best bid qty
  "a":"25.36520000",        // best ask price
  "A":"40.66000000"         // best ask qty
}
```

It is then parsed using the json library into a scala case class

```scala
  case class BinanceBookTicker(
                                symbol: String,
                                bidPrice: Double,
                                bidQuantity: Double,
                                askPrice: Double,
                                askQuantity: Double,
                                updateId: Long,
                                eventTime: Long,
                                transactionTime: Long,
                                payload: String,
                                key: Int
                              )
```

There are two additional keys in the class. Payload is used to keep the historical record of the json object and key is
added to do the keyby mapping which is set to 1 for all the case since we will be processing all the data in a single 
stream.

### Processing
Once the data is read and parsed into a single case class, it is used to calculate teh decayed sum. 
It uses  timestamp and the current bid/ask price information. It uses a tumbling window of the given sample period to 
calculate the value which is collected into the stream. Then we have used the filesink with rolling policy to create 
a new file for each minute under the specific folder for each symbol.
