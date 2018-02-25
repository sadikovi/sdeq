# sdeq
Recommender challenge _(Senior Data Engineer Question)_.

## Index
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Build](#build)
- [Test](#test)
- [Demo](#demo)

### Architecture

### Prerequisites
To build the project and run the demo, following things are required:
- **sbt**, see https://www.scala-sbt.org/1.0/docs/Setup.html for more information
- **Apache Spark 2.2.1**, you can download and install it from http://spark.apache.org/downloads.html
- **Docker**, see https://docs.docker.com/install/ for more information

> I use docker to run Mongo 3.2, if you have the database already running, then you do not
> need to run `./sbin/run-mongo.sh`, just update options to point to your database.

### Setup
Just clone repository and `cd` into project root
```sh
git clone https://github.com/sadikovi/sdeq.git
cd sdeq
```

### Build
For building the project and creating an assembly jar run `sbt assembly` from the project root.
To compile run `sbt compile` or `sbt 'test:compile'`.

### Test
For tests run `sbt test` from the project root.

### Demo
To run demo, follow the steps:
- run `sbt assembly` to build the jar (see [Build](#build) section)
- prepare Mongo DB by running `./sbin/run-mongo.sh` - this will launch container with Mongo
- open connection to simulate streaming data by running `./sbin/run-stream.sh`, prepare to type some data in!
- launch spark application, see commands and options below
- issue some http requests to get model predictions!

Start Spark application with this command (assuming that your current directory is the project root)
```sh
spark-submit \
  --conf spark.sdeq.history.files=./data \
  --class com.github.sadikovi.sdeq.Main \
  target/scala-2.11/sdeq-assembly-0.1.0-SNAPSHOT.jar
```

If you want to provide more options (see below) set them before the jar path, otherwise they will
be ignored, e.g.
```sh
spark-submit \
  --conf spark.sdeq.history.files=./data \
  --conf spark.sdeq.server.host=myhost \
  --conf spark.sdeq.server.port=7777 \
  --class com.github.sadikovi.sdeq.Main \
  target/scala-2.11/sdeq-assembly-0.1.0-SNAPSHOT.jar
```

Once you see the log message `ModelServer: Started model server on {HOST}:{PORT}`, you can issue
some get requests to get predictions. For example, open another terminal and run the following:
```sh
curl "localhost:28080/predict?customer=C1&product=P101" && echo ""
# example response
# Customer C1 viewed P101, so might also like: P201 (0.816496580927726), P131 (0.7071067811865475), P234 (0.7071067811865475)

curl "localhost:28080/predict?customer=C2&product=P201" && echo ""
# example response
# Customer C2 viewed P201, so might also like: P101 (0.816496580927726), P121 (0.816496580927726), P131 (0.5773502691896258)

# or try some non-existing product (we just report any 3)
curl "localhost:28080/predict?customer=C2&product=P999" && echo ""
# example response
# Customer C2 viewed P201, so might also like: P101 (0.0), P121 (0.0), P131 (0.0)
```

#### What's happening
This will load the data from history files, update model and start model server. Then it will listen
for the data stream opened by `./sbin/run-stream.sh` and load data into Mongo. Periodically, it will
retrain the model and cache in memory and also store it in Mongo.

#### More details
When you launch application, it will check if Mongo collections exist and will act accordingly. For
example, if `customer_product` collection exists, it assumes that history data has already been loaded,
so it will ignore that step. If `similarity` table already exists, it will load model, instead of
recomputing from the data (note that this does not affect periodic updates, we still want to
recompute the model during the lifetime of the application!).

#### Supported Spark options
Below is the list of options, note that all values are set to defaults, most of them, except history
files option should work out of the box.
Feel free to change them!
- `spark.sdeq.mongodb.uri`, Mongo URI to connect, default is `mongodb://localhost:27017`
- `spark.sdeq.server.host`, host for the model server, default is `localhost`
- `spark.sdeq.server.port`, port for the model server, default is `28080`
- `spark.sdeq.stream.host`, host for stream to connect to, default is `localhost`
- `spark.sdeq.stream.port`, port for stream to connect to `9999`
- `spark.sdeq.train.interval`, train/reload interval for the prediction model, default is 2 min, if
you run on the large data, consider increasing it - it is designed for semi-online training!
- `spark.sdeq.history.files`, directory (or a file) path with history data
