package esteco.ubies.importer.modelData;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.BsonTimestamp;

import java.util.List;

public class ConstellationMessage extends PowerMessage {
    private List<Integer> A0;

    public ConstellationMessage(int protocol, BsonTimestamp timestamp, List<Integer> a1, List<Integer> a2,
                                List<Integer> a3, List<Integer> a4, List<Integer> a5, List<Integer> a6, List<Integer> a7, List<Integer> a0) {
        super(protocol, timestamp, a1, a2, a3, a4, a5, a6, a7);
        A0 = a0;
    }

    public List<Integer> getA0() {
        return A0;
    }

    public void setA0(List<Integer> a0) {
        A0 = a0;
    }

    @Override
    public String toString(){
        try {
            // create `ObjectMapper` instance
            ObjectMapper mapper = new ObjectMapper();

            // create a JSON object
            ObjectNode jsonMessage = mapper.createObjectNode();
            jsonMessage.put("protocol", this.getProtocol());
            jsonMessage.put("timestamp", String.valueOf(this.getTimestamp()));
            //jsonMessage.put("timestamp", this.getTimestamp());
            jsonMessage.put("A0", A0.toString());
            jsonMessage.put("A1", this.getA1().toString());
            jsonMessage.put("A2", this.getA2().toString());
            jsonMessage.put("A3", this.getA3().toString());
            jsonMessage.put("A4", this.getA4().toString());
            jsonMessage.put("A5", this.getA5().toString());
            jsonMessage.put("A6", this.getA6().toString());
            jsonMessage.put("A7", this.getA7().toString());

            // convert `ObjectNode` to pretty-print JSON
            // without pretty-print, use `jsonMessage.toString()` method

            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonMessage);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
