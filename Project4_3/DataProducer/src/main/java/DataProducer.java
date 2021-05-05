import org.apache.kafka.clients.producer.Producer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.json.JSONObject;

public class DataProducer {
    private Producer<String, String> producer;
    private String traceFileName;

    public DataProducer(Producer producer, String traceFileName) {
        this.producer = producer;
        this.traceFileName = traceFileName;
    }

    /**
      Task 1:
        In Task 1, you need to read the content in the tracefile we give to you, 
        create two streams, and feed the messages in the tracefile to different 
        streams based on the value of "type" field in the JSON string.

        Please note that you're working on an ec2 instance, but the streams should
        be sent to your samza cluster. Make sure you can consume the topics on the
        master node of your samza cluster before you make a submission.
    */
    public void sendData() {
        BufferedReader reader = new BufferedReader(new FileReader(this.traceFileName));

        String strCurrentLine;
        while ((strCurrentLine = reader.readLine()) != null) {
            System.out.println(strCurrentLine);
        }
        
        JSONObject lineJson = new JSONObject(strCurrentLine);
        if(lineJson.getString("type").equals("DRIVER_LOCATION")){
            this.producer.send("driver-locations", strCurrentLine);
        }
        else if(lineJson.getString("type").equals("LEAVING_BLOCK")
        ||lineJson.getString("type").equals("ENTERING_BLOCK")
        ||lineJson.getString("type").equals("RIDE_REQUEST")
        ||lineJson.getString("type").equals("RIDE_COMPLETE")){
            this.producer.send(new ProducerRecord<String, String>("events", strCurrentLine));
        }

        reader.close();
    	producer.close();
    }

}
