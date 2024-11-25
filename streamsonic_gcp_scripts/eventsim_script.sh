#!/bin/bash

cd ~/StreamSonic/eventsim_docker

echo "Building Eventsim Image..."
docker build -t streamsonic .

echo "Running Eventsim in detached mode..."

# timeout 600s 
docker run -itd \
  --network host \
  --name million_events \
  streamsonic \
  -c "examples/example-config.json" \
  --start-time "2022-01-01T00:00:00" \
  --end-time "2022-12-31T23:59:59" \
  --nusers 100 \
  --kafkaBrokerList localhost:9092 \

echo "Started streaming events for users over the year 2022..."
echo "Eventsim is running in detached mode."
echo "Run 'docker logs --follow million_events' to see the logs."

