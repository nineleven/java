import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.IPipelineStep;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SemanticConfigValidator {

    private final HashMap<String, ConfigFieldType> fieldTypes;

    private final Logger logger;

    public enum ConfigFieldType {
        FT_IS_PRESENT,
        FT_EXISTING_FILE,
        FT_POSITIVE_INT,
        FT_CLASS_NAME,
        FT_PIPELINE
    }

    public SemanticConfigValidator(HashMap<String, ConfigFieldType> fieldTypes,
                                   Logger logger) {
        this.fieldTypes = fieldTypes;
        this.logger = logger;
    }

    private boolean validatePositiveInt(String value) {
        if (value == null) {
            return false;
        }
        try {
            int i = Integer.parseInt(value);
            if (i <= 0) {
                return false;
            }
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
             Class cl = Class.forName(value);
             return IPipelineStep.class.isAssignableFrom(cl);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private boolean validatePipeline(String value) {
        for (String worker : value.split(";")) {
            String[] workerParams = worker.split(",");

            if (workerParams.length != 2) {
                return false;
            }

            for (int i = 0; i < workerParams.length; ++i) {
                workerParams[i] = workerParams[i].trim();
            }

            if (!validateClassName(workerParams[0]) ||
                    !validateExistingFile(workerParams[1])) {
                return false;
            }

            try {
                Class cl = Class.forName(workerParams[0]);
                if (!IConfigurable.class.isAssignableFrom(cl) ||
                        !IPipelineStep.class.isAssignableFrom(cl)) {
                    return false;
                }
            } catch (ClassNotFoundException e) {
                return false;
            }
        }
        return true;
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
            case FT_POSITIVE_INT:
                isValid = validatePositiveInt(value);
                break;
            case FT_CLASS_NAME:
                isValid = validateClassName(value);
                break;
            case FT_PIPELINE:
                isValid = validatePipeline(value);
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
