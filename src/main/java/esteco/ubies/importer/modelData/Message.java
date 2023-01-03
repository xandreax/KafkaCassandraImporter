package esteco.ubies.importer.modelData;

import org.bson.BsonTimestamp;

public abstract class Message {
    private int protocol;
    //private Date timestamp;
    private BsonTimestamp timestamp;

    public Message(int protocol, BsonTimestamp timestamp) {
        this.protocol = protocol;
        /*int seconds = timestamp.getTime();
        int inc = timestamp.getInc();
        long millisecond = ((seconds* 1000000L) + inc)/1000;
        this.timestamp = new Date(millisecond);*/
        this.timestamp = timestamp;
    }

    /*public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(BsonTimestamp timestamp) {
        int seconds = timestamp.getTime();
        int inc = timestamp.getInc();
        long millisecond = ((seconds* 1000000L) + inc)/1000;
        this.timestamp = new Date(millisecond);
    }*/

    public BsonTimestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(BsonTimestamp timestamp) {
        this.timestamp = timestamp;
    }

    public int getProtocol() {
        return protocol;
    }

    public void setProtocol(int protocol) {
        this.protocol = protocol;
    }
}
