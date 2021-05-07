package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Map;
import java.util.HashMap;
import org.json.JSONObject;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

    /* Define per task state here. (kv stores etc)
       READ Samza API part in Primer to understand how to start
    */
    private int MAX_MONEY = 100;
    private KeyValueStore<String, String> driverloc;


    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe the kv stores?)
        driverloc = (KeyValueStore<String, String>)context.getTaskContext().getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        Map<String, Object> data = (Map<String, Object>) envelope.getMessage();


        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
        // Handle Driver Location messages
            int blockId = (int)data.get("blockId");
            int driverId = (int)data.get("driverId");
            double latitude = (double)data.get("latitude");
            double longitude = (double)data.get("longitude");

            String key = Integer.toString(blockId) + ',' + Integer.toString(driverId);
            //check if key exists
            JSONObject driver = driverloc.get(key) != null ? 
                new JSONObject(driverloc.get(key)) : new JSONObject();

            driver.put("latitude", latitude);
            driver.put("longitude", longitude);
            driverloc.put(key, driver.toString());            

        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
        // Handle Event messages
            String type = (String)data.get("type");

            if (type.equals("ENTERING_BLOCK")) {
                int blockId = (int)data.get("blockId");
                int driverId = (int)data.get("driverId");
                double latitude = (double)data.get("latitude");
                double longitude = (double)data.get("longitude");
                String gender = data.get("gender").toString();
                double rating = (double)data.get("rating");
                int salary = (int)data.get("salary");
                salary = salary > MAX_MONEY ? MAX_MONEY : salary;
                String status = data.get("status").toString();

                String key = Integer.toString(blockId) + ',' + Integer.toString(driverId);
                //check if key exists
                JSONObject driver = driverloc.get(key) != null ? 
                    new JSONObject(driverloc.get(key)) : new JSONObject();

                driver.put("latitude", latitude);
                driver.put("longitude", longitude);
                driver.put("gender", gender);
                driver.put("rating", rating);
                driver.put("salary", salary);
                driver.put("status", status);
                driverloc.put(key, driver.toString()); 

            } else if (type.equals("RIDE_COMPLETE")) {
                int blockId = (int)data.get("blockId");
                int driverId = (int)data.get("driverId");
                double latitude = (double)data.get("latitude");
                double longitude = (double)data.get("longitude");
                String gender = data.get("gender").toString();
                double rating = (double)data.get("rating");
                int salary = (int)data.get("salary");
                salary = salary > MAX_MONEY ? MAX_MONEY : salary;
                String status = "AVAILABLE";
                double user_rating = (double)data.get("user_rating");
                rating = (rating + user_rating) / 2;

                String key = Integer.toString(blockId) + ',' + Integer.toString(driverId);
                //check if key exists
                JSONObject driver = driverloc.get(key) != null ? 
                    new JSONObject(driverloc.get(key)) : new JSONObject();

                driver.put("latitude", latitude);
                driver.put("longitude", longitude);
                driver.put("gender", gender);
                driver.put("rating", rating);
                driver.put("salary", salary);
                driver.put("status", status);
                driverloc.put(key, driver.toString()); 

            } else if (type.equals("LEAVING_BLOCK")) {
                int blockId = (int)data.get("blockId");
                int driverId = (int)data.get("driverId");

                String key = Integer.toString(blockId) + ',' + Integer.toString(driverId);
                driverloc.delete(key);

            } else {
                int blockId = (int)data.get("blockId");
                int clientId = (int)data.get("clientId");
                double latitude = (double)data.get("latitude");
                double longitude = (double)data.get("longitude");
                String gender_preference = data.get("gender_preference").toString();

                KeyValueIterator<String, String> entries = driverloc.range(blockId + ",0", blockId + ",:");
                double maxScore = 0.0;
                int maxDriverId = 0;
                boolean hasDriver = false;

                while(entries.hasNext()){
                    Entry<String, String> entry = entries.next();
                    int driverId = Integer.valueOf(entry.getKey().split(",")[1]);
                    JSONObject driver = new JSONObject(entry.getValue());
                    String status = driver.getString("status");
                    if (status.equals("UNAVAILABLE")) {
                        continue;
                    }
                    hasDriver = true;

                    double score = score(latitude, longitude, gender_preference, driver);
                    if (score > maxScore){
                        maxScore = score;
                        maxDriverId = driverId;
                    }
                }

                entries.close();

                if (hasDriver) {
                    String key = Integer.toString(blockId) + ',' + Integer.toString(maxDriverId);
                    JSONObject driver = new JSONObject(driverloc.get(key));
                    driver.put("status", "UNAVAILABLE");
                    driverloc.put(key, driver.toString()); 

                    Map<String, Object> message = new HashMap<String, Object>();
                    message.put("clientId", clientId);
                    message.put("driverId", maxDriverId);
                    collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, message));
                }
            }

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    private double score(double latitude, double longitude, String gender_preference, JSONObject driver){
        double latitude2 = driver.getDouble("latitude");
        double longitude2 = driver.getDouble("longitude");
        String gender = driver.getString("gender");
        double rating = driver.getDouble("rating");
        int salary = driver.getInt("salary");
        salary = salary > MAX_MONEY ? MAX_MONEY : salary;

        double d = Math.exp(-1*Math.sqrt((latitude2 - latitude) * (latitude2 - latitude) 
            + (longitude2 - longitude) * (longitude2 - longitude)));
        double r = rating/5;
        double s = 1 - salary/100.0;
        double g = gender_preference.equals(gender) || gender_preference.equals("N") ? 1.0 : 0.0;

        return d * 0.4 + g * 0.1 + r * 0.3 + s * 0.2;
    }
}
