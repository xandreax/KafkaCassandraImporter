package esteco.ubies.importer.modelData;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.BsonTimestamp;

import java.util.List;

public class TagMessage extends Message {
    private int id;
    private double temp;
    private double battery;
    private List<Integer> position;
    private List<Integer> hposition;
    private List<Double> acc;
    private List<Double> gyro;

    public TagMessage(int protocol, BsonTimestamp timestamp, int id, double temp, double battery,
                      List<Integer> position,
                      List<Integer> hposition,
                      List<Double> acc, List<Double> gyro) {
        super(protocol, timestamp);
        this.id = id;
        this.temp = temp;
        this.battery = battery;
        this.position = position;
        this.hposition = hposition;
        this.acc = acc;
        this.gyro = gyro;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    public double getBattery() {
        return battery;
    }

    public void setBattery(double battery) {
        this.battery = battery;
    }

    public List<Integer> getPosition() {
        return position;
    }

    public void setPosition(List<Integer> position) {
        this.position = position;
    }

    public List<Integer> getHposition() {
        return hposition;
    }

    public void setHposition(List<Integer> hposition) {
        this.hposition = hposition;
    }

    public List<Double> getAcc() {
        return acc;
    }

    public void setAcc(List<Double> acc) {
        this.acc = acc;
    }

    public List<Double> getGyro() {
        return gyro;
    }

    public void setGyro(List<Double> gyro) {
        this.gyro = gyro;
    }

    public void printMessage(String json) {
        System.out.println(json);
    }

    @Override
    public String toString(){
        try {
            // create `ObjectMapper` instance
            ObjectMapper mapper = new ObjectMapper();

            // create a JSON object
            ObjectNode jsonMessage = mapper.createObjectNode();
            jsonMessage.put("protocol", this.getProtocol());
            jsonMessage.put("id", id);
            jsonMessage.put("temp", temp);
            jsonMessage.put("battery", battery);
            jsonMessage.put("position", position.toString());
            jsonMessage.put("height position", hposition.toString());
            jsonMessage.put("timestamp", String.valueOf(this.getTimestamp()));
            //jsonMessage.put("timestamp", this.getTimestamp());
            jsonMessage.put("acc", acc.toString());
            jsonMessage.put("gyro", gyro.toString());

            // convert `ObjectNode` to pretty-print JSON
            // without pretty-print, use `jsonMessage.toString()` method

            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonMessage);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
