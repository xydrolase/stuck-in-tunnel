# :bus: stuck-in-tunnel

A service to (eventually) provide predictions on how long a NJ Transit Bus will be stuck in the notorious I-495 and Lincoln Tunnel, built with [fs2](https://fs2.io) and [http4s](https://http4s.org).

## Overview
Currently, this project mainly consists of a crawler that utilizes [MyBusNow](https://mybusnow.njtransit.com/) API to track real-time bus locations.
The fs-2 streaming pipeline has the following stages:

 - [x] Track vehicle locations of given bus routes.
 - [x] Track vehicle movements within 10-second windows.
 - [x] Compute the bus stops visited during each time window.
 - [x] Dump vehicle location and bus stop arrival data points to data files using Arrow IPC format.
 - [ ] Sync data files to cloud storage (e.g. AWS S3, Google Cloud Storage)
 - [ ] Compute statistics of travel time between two arbitrary bus stops along a given route.

Within this project we are also incubating [`sarrow`](src/main/scala/sarrow), a Scala library to use Arrow based on the [Java library](https://arrow.apache.org/docs/java/).
When `sarrow` matures, it will be forklifted into its own library.

## ToDos
 - [ ] Offline ETL pipeline to convert Arrow IPC format to Parquet files.
 - [ ] Build predictive model to forecast travel time between bus stops along each route given time, day of week, traffic conditions etc.
 - [ ] Web services to publish real-time stats and forecasts.

