import ru.spbstu.pipeline.RC;

import java.util.HashMap;
import java.util.logging.Logger;

public class Config {

    private HashMap<String, String> fields;

    private final Logger logger;

    private Config(HashMap<String, String> fields, Logger logger)
    {
        this.fields = fields;
        this.logger = logger;
    }

    public String getParameter(String key) {
        String value;
        try {
            value = fields.get(key);
        }
        catch (ClassCastException | NullPointerException ex) {
            logger.warning("Failed to parse a config parameter");
            value = null;
        }
        return value;
    }

    public Integer getIntParameter(String key) {
        String stringParameter = getParameter(key);
        if (stringParameter == null) {
            logger.warning("Failed to parse a config parameter " + key);
            return null;
        }
        Integer value;
        try {
            value = Integer.parseInt(stringParameter);
        }
        catch (NumberFormatException ex) {
            value = null;
            logger.warning("Failed to parse a config parameter " + key);
        }
        return value;
    }

    public static Pair<Config, RC> fromFile(String filename, String delimiter, Logger logger) {
        Pair<HashMap<String, String>, RC> res = FileParser.readMap(filename, delimiter, logger);

        if (res.first == null) {
            logger.severe("Failed to read a map from " + filename);
            return new Pair<>(null, res.second);
        }

        Config cfg = new Config(res.first, logger);

        return new Pair<>(cfg, RC.CODE_SUCCESS);
    }
}