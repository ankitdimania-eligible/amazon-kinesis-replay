package com.eligible.kinesis.replay;

import com.amazonaws.regions.Regions;
import com.amazonaws.samples.kinesis.replay.StreamPopulator;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.time.Instant;
import java.util.Map;

public class ReplayEventsLambda implements RequestHandler<Map<String, String>, String> {
    private static final String DEFAULT_REGION_NAME = Regions.getCurrentRegion() == null ? "us-east-1" : Regions.getCurrentRegion().getName();

    @Override
    public String handleRequest(Map<String, String> event, Context context) {
        LambdaLogger logger = context.getLogger();

        Instant seekToEpoch = null;
        if (event.containsKey("seek")) {
            seekToEpoch = Instant.parse(event.get("seek"));
        }

        StreamPopulator populator = new StreamPopulator(
                event.getOrDefault("bucketRegion", "us-east-1"),
                event.getOrDefault("bucketName", "aws-bigdata-blog"),
                event.getOrDefault("objectPrefix", "artifacts/kinesis-analytics-taxi-consumer/taxi-trips.json.lz4/"),
                event.getOrDefault("streamRegion", DEFAULT_REGION_NAME),
                event.getOrDefault("streamName", "taxi-trip-events"),
                event.containsKey("aggregate"),
                event.getOrDefault("timestampAttributeName", "dropoff_datetime"),
                Float.parseFloat(event.getOrDefault("speedup", "6480")),
                Long.parseLong(event.getOrDefault("statisticsFrequency", "20000")),
                event.containsKey("noWatermark"),
                seekToEpoch,
                Integer.parseInt(event.getOrDefault("bufferSize", "100000")),
                Integer.parseInt(event.getOrDefault("maxOutstandingRecords", "10000")),
                event.containsKey("noBackpressure")
        );

        populator.populate();
        return "200 OK";
    }
}
