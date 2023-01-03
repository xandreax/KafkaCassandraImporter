package esteco.ubies.server.utils;

public class CollectionNameValidator {
    public static boolean collectionNameIsValid(String collectionName) {
        if(collectionName.length() >= 8){
            if(collectionName.substring(0, 7).equals("system."))
                return false;
        }
        return !collectionName.equals("") && !collectionName.matches("$");
    }
}
