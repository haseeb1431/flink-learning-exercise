## Use the taxi data streams

These exercises use data [generators](/common/sources) that produce simulated event streams.
The data is inspired by the [New York City Taxi & Limousine Commission's](http://www.nyc.gov/html/tlc/html/home/home.shtml) public
[data set](https://uofi.app.box.com/NYCtaxidata) about taxi rides in New York City.

### Schema of taxi ride events

Our taxi data set contains information about individual taxi rides in New York City. Each ride is represented by two events: a trip start, and a trip end.

Each taxi ride event consists of eleven fields:

```
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
isStart        : Boolean   // TRUE for ride start events, FALSE for ride end events
eventTime      : Instant   // the timestamp for this event
startLon       : Float     // the longitude of the ride start location
startLat       : Float     // the latitude of the ride start location
endLon         : Float     // the longitude of the ride end location
endLat         : Float     // the latitude of the ride end location
passengerCnt   : Short     // number of passengers on the ride
```

### Schema of taxi fare events

There is also a related data set containing fare data about those same rides, with the following fields:

```
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
startTime      : Instant   // the start time for this ride
paymentType    : String    // CASH or CARD
tip            : Float     // tip for this ride
tolls          : Float     // tolls for this ride
totalFare      : Float     // total fare collected
```

## How to do the project exercises

In the hands-on sessions, you will implement Flink programs using various Flink APIs.

It assumes you have set up your development environment already as we have been learning throughout the course

### Learn about the data

The initial set of exercises are all based on data streams of events about taxi rides and taxi fares. These streams are produced by source functions which reads data from input files.
Read the [instructions](#using-the-taxi-data-streams) to learn how to use them.


### Run and debug Flink programs in your IDE

Flink programs can be executed and debugged from within an IDE. This significantly eases the development process and provides an experience similar to working on any other Java (or Scala) application.

To start a Flink program in your IDE, run its `main()` method. Under the hood, the execution environment will start a local Flink instance within the same process. Hence, it is also possible to put breakpoints in your code and debug it.

## Project exercises


### Task: Creating Models
Create the classes for the `TaxiRide` and `TaxiFare` in the models folders. Think about that whether it should be a
case class or a normal class

### Task: Creating Data source 
create 2 data sources for the `TaxiRideGenerator` and `TaxiFareGenrator` and use them to print the data on the console

###Task: Filtering a Stream (Ride Cleansing)

The task of the "Taxi Ride Cleansing" exercise is to cleanse a stream of TaxiRide events by removing events that start or end outside of New York City.

The `GeoUtils` utility class provides a static method `isInNYC(float lon, float lat)` to check if a location is within the NYC area.

#### Input Data

This exercise is based on a stream of `TaxiRide` events, as described in [Using the Taxi Data Streams](../README.md#using-the-taxi-data-streams).

#### Expected Output

The result of the exercise should be a `DataStream<TaxiRide>` that only contains events of taxi rides which both start and end in the New York City area as defined by `GeoUtils.isInNYC()`.

The resulting stream should be printed to standard out.

### Task: Filtering
* Filter all streams where the ride is ongoing right now

### Task: Key By

* Find the number of rides for each driver

### Task: Key by with filter

* Find the total tips for a driver

### Task:Ride count (implicit state/stateful aggregation)

The task of the "Ride count " exercise is to create a stream of DriverId and their total rides.

Note that this is implicitly keeping state for each driver. This sort of simple, non-windowed
 aggregation on an unbounded set of keys will use an unbounded amount of state.

#### Input Data

This exercise is based on a stream of `TaxiRide` events, as described in [Using the Taxi Data Streams](../README.md#using-the-taxi-data-streams).

#### Expected Output

The result of the exercise should be a `DataStream<Tuple2<Long, Long>> ` that  counts the rides for each driver.

The resulting stream should be printed to standard out.



### Task: Stateful Enrichment (Rides and Fares)

The goal of this exercise is to join together the `TaxiRide` and `TaxiFare` records for each ride.

For each distinct `rideId`, there are exactly three events:

1. a `TaxiRide` START event
1. a `TaxiRide` END event
1. a `TaxiFare` event (whose timestamp happens to match the start time)

The result should be a `DataStream<RideAndFare>`, with one record for each distinct `rideId`. Each tuple should pair the `TaxiRide` START event for some `rideId` with its matching `TaxiFare`.

#### Input Data

For this exercise you will work with two data streams, one with `TaxiRide` events generated by a `TaxiRideSource` and the other with `TaxiFare` events generated by a `TaxiFareSource`. 

#### Expected Output

The result of this exercise is a data stream of `RideAndFare` records, one for each distinct `rideId`. The exercise is setup to ignore the END events, and you should join the event for the START of each ride with its corresponding fare event.

Once you have both the ride and fare that belong together, you can create the desired object for the output stream by creating a new object something like

`new RideAndFare(ride, fare)`

The stream will be printed to standard out.


### Windowed Analytics (Hourly Tips)

The task of the "Hourly Tips" exercise is to identify, for each hour, the driver earning the most tips. It's easiest to approach this in two steps: first use hour-long windows that compute the total tips for each driver during the hour, and then from that stream of window results, find the driver with the maximum tip total for each hour.

Please note that the program should operate in event time.

#### Input Data

The input data of this exercise is a stream of `TaxiFare` events generated by the [Taxi Fare Stream Generator](../README.md#using-the-taxi-data-streams).

The `TaxiFareGenerator` annotates the generated `DataStream<TaxiFare>` with timestamps and watermarks. Hence, there is no need to provide a custom timestamp and watermark assigner in order to correctly use event time.

#### Expected Output

The result of this exercise is a data stream of `Tuple3<Long, Long, Float>` records, one for each hour. Each hourly record should contain the timestamp at the end of the hour, the driverId of the driver earning the most in tips during that hour, and the actual total of their tips.

The resulting stream should be printed to standard out.


### Lab: `ProcessFunction` and Timers (Long Ride Alerts)

The goal of the "Long Ride Alerts" exercise is to provide a warning whenever a taxi ride
lasts for more than two hours.

This should be done using the event time timestamps and watermarks that are provided in the data stream.

The stream is out-of-order, and it is possible that the END event for a ride will be processed before
its START event.

An END event may be missing, but you may assume there are no duplicated events, and no missing START events.

It is not enough to simply wait for the END event and calculate the duration, as we want to be alerted
about the long ride as soon as possible.

You should eventually clear any state you create.

#### Input Data

The input data of this exercise is a `DataStream` of taxi ride events.

#### Expected Output

The result of the exercise should be a `DataStream<LONG>` that contains the `rideId` for rides
with a duration that exceeds two hours.

The resulting stream should be printed to standard out.

