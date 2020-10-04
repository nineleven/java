import java.util.HashMap;

public class Config {

    private HashMap<String, String> fields;

    private Config(HashMap<String, String> fields) {
        this.fields = fields;
    }

    public String getParameter(String key) {
        String value;
        try {
            value = fields.get(key);
        }
        catch (ClassCastException | NullPointerException ex) {
            value = null;
        }
        return value;
    }

    /*
     reads a config from file assuming the default delimiter "="
     see fromFile(String filename, String delimiter) for details
     */
    public static Config fromFile(String filename) {
        return fromFile(filename, "=");
    }

    /*
     reads a config from file, each nonempty line of which is in the following format:
     key[any number of space characters]delimiter[any number of space characters]value
     returns null in case of IO exception or if a line of wrong format was found
     */
    public static Config fromFile(String filename, String delimiter) {
        Config config;

        HashMap<String, String> fields = FileParser.readMap(filename, delimiter);

        if (fields == null) {
            config = null;
        } else {
            config = new Config(fields);
        }

        return config;
    }
}