package ru.spbstu.timofeev.config;

import java.io.File;
import java.util.logging.Logger;

public abstract class BaseSemantics {

    private final Logger logger;
    protected BaseSemantics(Logger logger) {
        this.logger = logger;
    }

    public abstract boolean validateField(String fieldName, String fieldValue);

    protected Logger getLogger() {
        return logger;
    }

    public static boolean validatePositiveInt(String value) {
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

    public static boolean validateExistingFile(String value) {
        if (value == null) {
            return false;
        }
        return new File(value).exists();
    }
}
