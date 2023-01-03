package esteco.ubies.importer.modelData;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.BsonTimestamp;

import java.util.List;

public class PowerMessage extends Message {

    private List<Integer> A1;
    private List<Integer> A2;
    private List<Integer> A3;
    private List<Integer> A4;
    private List<Integer> A5;
    private List<Integer> A6;
    private List<Integer> A7;

    public PowerMessage(int protocol, BsonTimestamp timestamp, List<Integer> a1, List<Integer> a2, List<Integer> a3,
                        List<Integer> a4, List<Integer> a5, List<Integer> a6, List<Integer> a7) {
        super(protocol, timestamp);
        A1 = a1;
        A2 = a2;
        A3 = a3;
        A4 = a4;
        A5 = a5;
        A6 = a6;
        A7 = a7;
    }

    public List<Integer> getA1() {
        return A1;
    }

    public void setA1(List<Integer> a1) {
        A1 = a1;
    }

    public List<Integer> getA2() {
        return A2;
    }

    public void setA2(List<Integer> a2) {
        A2 = a2;
    }

    public List<Integer> getA3() {
        return A3;
    }

    public void setA3(List<Integer> a3) {
        A3 = a3;
    }

    public List<Integer> getA4() {
        return A4;
    }

    public void setA4(List<Integer> a4) {
        A4 = a4;
    }

    public List<Integer> getA5() {
        return A5;
    }

    public void setA5(List<Integer> a5) {
        A5 = a5;
    }

    public List<Integer> getA6() {
        return A6;
    }

    public void setA6(List<Integer> a6) {
        A6 = a6;
    }

    public List<Integer> getA7() {
        return A7;
    }

    public void setA7(List<Integer> a7) {
        A7 = a7;
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
            jsonMessage.put("A1", A1.toString());
            jsonMessage.put("A2", A2.toString());
            jsonMessage.put("A3", A3.toString());
            jsonMessage.put("A4", A4.toString());
            jsonMessage.put("A5", A5.toString());
            jsonMessage.put("A6", A6.toString());
            jsonMessage.put("A7", A7.toString());

            // convert `ObjectNode` to pretty-print JSON
            // without pretty-print, use `jsonMessage.toString()` method

            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonMessage);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

}
