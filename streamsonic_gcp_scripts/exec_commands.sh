#!/bin/bash

# Send messages to Kafka Topic from eventsim
# Timeout set to 10 minutes (600 seconds)
timeout 600s docker run -itd \
  --network host \
  --name events_example \
  --memory="5.5g" \
  --memory-swap="7g" \
  --oom-kill-disable \
  streamsonic \
    -c "examples/example-config.json" \
    --start-time "2022-03-20T10:10:00" \
    --end-time "2022-03-20T18:00:00" \
    --nusers 1500000 \
    --growth-rate 10 \
    --userid 1 \
    --kafkaBrokerList localhost:9092 \
    --randomseed 1 \
    # --continuous

timeout 600s docker run -itd \
  --network host \
  --name events_alt \
  --memory="5.5g" \
  --memory-swap="7g" \
  --oom-kill-disable \
  streamsonic \
    -c "examples/example-config.json" \
    --start-time "2022-03-20T10:10:00" \
    --end-time "2022-03-20T18:00:00" \
    --nusers 1500000 \
    --growth-rate 9 \
    --userid 3000000 \
    --kafkaBrokerList localhost:9092 \
    --randomseed 2 \
    # --continuous

timeout 600s docker run -itd \
  --network host \
  --name events_new \
  --memory="2.5g" \
  --memory-swap="6g" \
  --oom-kill-disable \
  streamsonic \
    -c "examples/example-config.json" \
    --start-time "2022-03-20T08:20:00" \
    --end-time "2022-03-20T18:00:00" \
    --nusers 900000 \
    --growth-rate 12 \
    --userid 6000000 \
    --kafkaBrokerList localhost:9092 \
    --randomseed 3 \
    # --continuous

# Attach logs if necessary
docker logs --follow events_example
docker logs --follow events_alt


export KAFKA_ADDRESS=
export GCS_STORAGE_PATH=gs://streamify

#Stream page view events into Spark
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 \
stream_page_view_events.py

#Stream listen events into Spark
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
stream_listen_events.py

#Stream all events into Spark
nohup spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
stream_all_events.py \
> nohup.out 2>&1 &



# Running another Eventsim container (one-off) with timeout
timeout 600s docker run -it \
  --network host \
  streamsonic \
    -c "examples/example-config.json" \
    --start-time "2022-03-25T17:40:00" \
    --end-time "2022-03-25T23:00:00" \
    --nusers 1000000 \
    --growth-rate 10 \
    --userid 1 \
    --kafkaBrokerList localhost:9092 \
    --randomseed 1 \
    # --continuous
