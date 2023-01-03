package esteco.ubies.importer.exceptions;

public class InvalidMessageException extends Exception {
    public InvalidMessageException(String key) {
        super(key + " is not a valid message");
    }
}
