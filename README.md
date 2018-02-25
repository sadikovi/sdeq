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
The goal is a bit vague based on the pdf description. I took an opportunity to summarize it and
make it a little more interesting.

Our goal is to build item-to-item based recommender system that should perform following functions:
- On cold start, load data from historical files, it can be a single file or a partitioned directory
of files, does not matter. The only requirement is that each file is a CSV file of following structure
```
customer,product
C1,P101
C1,P121
...
```
Files also can contain duplicates.
- On any subsequent start, we should not load data every time.
- Recommendation system should be retrained offline, periodically, or online, whichever works better.
- Model should be served via http requests, e.g. to display on a web page.
- System in theory should scale to 50 million customers and tens of thousands of products.

#### In theory
This is what we are trying to build.
```
    +---------------+              +----------------------------------+
    | History files |              | Stream of recent views or clicks |
    +---------------+              | (a message bus, e.g. Kafka)      |
     Load on startup               +----------------------------------+
          |                                    |                   ^
          |    +-------------------+           |                   |
          +--> | Execution engine  | <---------+                  ... Data collected into Kafka
               | Batch + streaming |                               |
               +-------------------+                               |
              We periodically train model                          |
                        |      |                                   |
                        |      |        +--------------+  GET   +-------------+
                        |      +------> | Model server |------> | Web browser |
                        |               +--------------+        +-------------+
                        |
              +-------------------+
              | Scalable database |
              | NoSQL or RDBMS    |
              +-------------------+
```

We could go for some Lambda architecture, but it is overcomplicating things for this simple example
of only one model and not particularly large input data. Of course, for companies like Amazon or
Netflix, when there are dozens of systems in between, not this simple diagram.

#### Why this scales
For execution engine we use Apache Spark, no need to explain why it scales as long as code is written
to take advantage of a cluster and parallelize operations as much as possible. We also cache model
in memory to improve latency for the prediction queries.

Both customer-product data and item-to-item data are stored in the database. We use Mongo, because
it is just easy to set up and deal with, but an RDBMS could also work here, for example, sharded
PostgreSQL or MySQL.

Kafka gives high throughput and large volume processing. For our project at-least-once semantics
are okay, Kafka would work great - when computing similarity index we reduce to unique records.

#### Simplifications for this project
I simplified it a little bit. For example, I do not use Kafka in the demo, because, frankly, it is
quite a task on its own to set it up properly:). Instead we use simple socket stream, very minor
changes are required to connect to Kafka (couple of lines of code). We also use simple http server
to serve requests. In production you would want something more robust - fortunately, there are a lot
of frameworks out there for this.

> Note that it is important to sort of design it around web server, or have it as a completely
> different entity, otherwise it might be annoying to integrate later.

#### What I would change
- History data could be kept as Parquet files, so a job could periodically archive database data
into this format.
- Load history data based on missing records, not only once on a startup.
- Update web server to something more robust and functional, add a load balancer.
- Handle interesting cases like new customer, visited items and/or new products, right now we would
return arbitrary items, if we do not have score for it.
- Store only unique customer-product data to speed up computations.
- Use Kafka instead of simple socket.

### Prerequisites
To build the project and run the demo, following things are required:
- **sbt**, see https://www.scala-sbt.org/1.0/docs/Setup.html for more information
- **Apache Spark 2.2.1**, you can download and install it from http://spark.apache.org/downloads.html
- **Docker**, see https://docs.docker.com/install/ for more information

> I use docker to run Mongo 3.2, if you have the database already running, then you do not
> need to run `./sbin/run-mongo.sh`, just update options to point to your database.

### Setup
Clone repository and `cd` into project root
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
# Customer C1 viewed P101, so might also like: P201 (0.816496580927726), P131 (0.7071067811865475), P121 (0.4999999999999999)

curl "localhost:28080/predict?customer=C2&product=P201" && echo ""
# example response
# Customer C2 viewed P201, so might also like: P101 (0.816496580927726), P121 (0.816496580927726), P131 (0.5773502691896258)

# or try some non-existing product (we just report any 3)
curl "localhost:28080/predict?customer=C2&product=P999" && echo ""
# example response
# Customer C2 viewed P999, so might also like: P201 (0.0), P121 (0.0), P131 (0.0)
```

To add more data into database from a stream, type something like this into `./sbin/run-stream.sh`
terminal:
```sh
[Type record and hit Enter, e.g. 'C1,P234']
C4,P234
C1,P234
```
Wait for the next model update (approx. 2 min) to query the changes!

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
