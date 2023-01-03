package esteco.ubies.importer.exceptions;

public class CollectionNotFoundException extends Exception {
    public CollectionNotFoundException(String key) {
        super(key + ": not found this registration in the database");
    }
}
