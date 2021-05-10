package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Arrays;


/**
 * Consumes the stream of events.
 * Outputs a stream which handles static file and one stream
 * and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */

    private KeyValueStore<Integer, Map<String, Object>> userInfo;

    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    private Set<String> lowCalories;

    private Set<String> energyProviders;

    private Set<String> willingTour;

    private Set<String> stressRelease;

    private Set<String> happyChoice;

    private void initSets() {
        lowCalories = new HashSet<>(Arrays.asList("seafood", "vegetarian", "vegan", "sushi"));
        energyProviders = new HashSet<>(Arrays.asList("bakeries", "ramen", "donuts", "burgers",
                "bagels", "pizza", "sandwiches", "icecream",
                "desserts", "bbq", "dimsum", "steak"));
        willingTour = new HashSet<>(Arrays.asList("parks", "museums", "newamerican", "landmarks"));
        stressRelease = new HashSet<>(Arrays.asList("coffee", "bars", "wine_bars", "cocktailbars", "lounges"));
        happyChoice = new HashSet<>(Arrays.asList("italian", "thai", "cuban", "japanese", "mideastern",
                "cajun", "tapas", "breakfast_brunch", "korean", "mediterranean",
                "vietnamese", "indpak", "southern", "latin", "greek", "mexican",
                "asianfusion", "spanish", "chinese"));
    }

    // Get store tag
    private String getTag(String cate) {
        String tag = "";
        if (happyChoice.contains(cate)) {
            tag = "happyChoice";
        } else if (stressRelease.contains(cate)) {
            tag = "stressRelease";
        } else if (willingTour.contains(cate)) {
            tag = "willingTour";
        } else if (energyProviders.contains(cate)) {
            tag = "energyProviders";
        } else if (lowCalories.contains(cate)) {
            tag = "lowCalories";
        } else {
            tag = "others";
        }
        return tag;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize kv store
        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getTaskContext().getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("yelp-info");

        //Initialize store tags set
        initSets();

        //Initialize static data and save them in kv store
        initialize("UserInfoData.json", "NYCstore.json");
    }

    /**
     * This function will read the static data from resources folder
     * and save data in KV store.
     * <p>
     * This is just an example, feel free to change them.
     */
    public void initialize(String userInfoFile, String businessFile) {
        List<String> userInfoRawString = AdMatchConfig.readFile(userInfoFile);
        System.out.println("Reading user info file from " + Resources.getResource(userInfoFile).toString());
        System.out.println("UserInfo raw string size: " + userInfoRawString.size());
        for (String rawString : userInfoRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                int userId = (Integer) mapResult.get("userId");
                userInfo.put(userId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse user info :" + rawString);
            }
        }

        List<String> businessRawString = AdMatchConfig.readFile(businessFile);

        System.out.println("Reading store info file from " + Resources.getResource(businessFile).toString());
        System.out.println("Store raw string size: " + businessRawString.size());

        for (String rawString : businessRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                String storeId = (String) mapResult.get("storeId");
                String cate = (String) mapResult.get("categories");
                String tag = getTag(cate);
                mapResult.put("tag", tag);
                yelpInfo.put(storeId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse store info :" + rawString);
            }
        }
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

        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            if (type.equals("RIDER_INTEREST")) {
                int userId = (int)data.get("userId");
                String interest = data.get("interest").toString();
                int duration = (int)data.get("duration");

                //check if key exists
                JSONObject user = userInfo.get(userId) != null ? 
                    new JSONObject(userInfo.get(userId)) : new JSONObject();

                if (duration > 5) {
                    user.put("interest", interest);
                    userInfo.put(userId, user.toString());
                }

            } else if (type.equals("RIDER_STATUS")) {
                int userId = (int)data.get("userId");
                int mood = (int)data.get("mood");
                int bloodSugar = (int)data.get("bloodSugar");
                int stress = (int)data.get("stress");
                int active = (int)data.get("active");

                //check if key exists
                JSONObject user = userInfo.get(userId) != null ? 
                    new JSONObject(userInfo.get(userId)) : new JSONObject();
                
                JSONObject tags = new JSONObject();

                boolean other = true;
                if (bloodSugar > 4 && mood > 6 && active == 3) {
                    tags.put("lowCalories", 1);
                    other = false;
                } else {
                    tags.put("lowCalories", 0);
                }
                if (bloodSugar < 2 || mood < 4) {
                    tags.put("energyProviders", 1);
                    other = false;
                } else {
                    tags.put("energyProviders", 0);
                }
                if (active == 3) {
                    tags.put("willingTour", 1);
                    other = false;
                } else {
                    tags.put("willingTour", 0);
                }
                if (stress > 5 || active == 1 || mood < 4) {
                    tags.put("stressRelease", 1);
                    other = false;
                } else {
                    tags.put("stressRelease", 0);
                }
                if (mood > 6) {
                    tags.put("happyChoice", 1);
                    other = false;
                } else {
                    tags.put("happyChoice", 0);
                }
                if (other) {
                    tags.put("other", 1);
                } else {
                    tags.put("other", 0);
                }
                
                user.put("tags", tags);
                userInfo.put(userId, user.toString());
            } else if (type.equals("RIDE_REQUEST")) {
                int clientId = (int)data.get("clientId");
                double latitude = (double)data.get("latitude");
                double longitude = (double)data.get("longitude");

                JSONObject user = userInfo.get(clientId) != null ? 
                new JSONObject(userInfo.get(clientId)) : new JSONObject();

                JSONObject userTags = new JSONObject();
                String userInterest = user.getString("interest");
                int travel_count = user.getInt("travel_count");
                String device = user.getString("device");

                if (user.has("tags")) {
                    userTags = user.getJSONObject("tags");
                } else {
                    userTags.put("other", 1);
                }

                KeyValueIterator<String, String> entries = yelpInfo.all;
                double maxScore = 0.0;
                int maxStoreId = 0;
                String maxStoreName = "";

                while (entries.hasNext()) {
                    Entry<String, String> entry = entries.next();
                    String storeId = entry.getKey();
                    JSONObject store = new JSONObject(entry.getValue());

                    String name = store.getString("name");
                    int reviewCount = store.getInt("review_count");
                    String categories = store.getString("categories");
                    String storeTag = getTag(categories);
                    double rating = store.getDouble("rating");
                    String price = store.getString("price");
                    double latitude2 = store.getDouble("latitude");
                    double longitude2 = store.getDouble("longitude");

                    //step 1
                    if (userTags.getInt(storeTag) == 0) {
                        continue;
                    }

                    //step 2
                    double score = reviewCount * rating;

                    //step 3
                    if (categories.equals(userInterest)) {
                        score += 10;
                    }

                    //step 4
                    double priceFactore = 1.0;

                    if (score > maxScore) {
                        maxScore = score;
                        maxStoreId = storeId;
                        maxStoreName = name;
                    }
                }

                entries.close();

                Map<String, Object> message = new HashMap<String, Object>();
                message.put("userId", clientId);
                message.put("storeId", maxStoreId);
                message.put("name", maxStoreName);
                collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, message));
            }

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
}
