import ru.spbstu.pipeline.IPipelineStep;

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
        FT_CLASS_NAME
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

    private boolean validateClassName(String value) {
        try {
             Class clazz = Class.forName(value);
             return IPipelineStep.class.isAssignableFrom(clazz);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private boolean validateFieldType(String value, ConfigFieldType expectedType) {
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
            case FT_CLASS_NAME:
                isValid = validateClassName(value);
                break;
            default:
                logger.warning("Unknown config field type:" + expectedType);
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
                return false;
            }

            if (!validateFieldType(value, entry.getValue())) {
                logger.warning("Invalid field value " + value);
                return false;
            }
        }

        return true;
    }

}
