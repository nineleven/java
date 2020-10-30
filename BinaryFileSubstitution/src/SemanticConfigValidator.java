import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SemanticConfigValidator {

    private HashMap<String, ConfigFieldType> fieldTypes;

    private Logger logger;

    public enum ConfigFieldType {
        FT_IS_PRESENT,
        FT_EXISTING_FILE,
        FT_INT,
    }

    public SemanticConfigValidator(HashMap<String, ConfigFieldType> fieldTypes,
                                   Logger logger) {
        this.fieldTypes = fieldTypes;
        this.logger = logger;
    }

    private boolean validateInt(String value) {
        if (value == null) {
            return false;
        }
        try {
            Integer.parseInt(value);
        }
        catch (NumberFormatException ex) {
            return false;
        }
        return true;
    }

    private boolean validateExistingFile(String value) {
        if (value == null) {
            return false;
        }
        return new File(value).exists();
    }

    private boolean validatePresence(String value) {
        return true;
    }

    private boolean validateFieldType(String key, String value, ConfigFieldType expectedType) {
        boolean isValid = true;

        switch (expectedType) {
            case FT_IS_PRESENT:
                isValid = validatePresence(value);
                break;
            case FT_EXISTING_FILE:
                isValid = validateExistingFile(value);
                break;
            case FT_INT:
                isValid = validateInt(value);
                break;
            default:
                // log warning
                break;
        }

        return isValid;
    }

    public boolean validate(Config cfg) {
        for(Map.Entry<String, ConfigFieldType> entry : fieldTypes.entrySet()) {

            String value = cfg.getParameter(entry.getKey());
            if (value == null) {
                // log warning
                logger.warning("Config missing field " + entry.getKey());
                continue;
            }

            if (!validateFieldType(entry.getKey(), value, entry.getValue())) {
                logger.warning("Invalid field value " + value);
                return false;
            }
        }

        return true;
    }

}
