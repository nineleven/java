package ru.spbstu.timofeev.config;

import ru.spbstu.pipeline.RC;
import ru.spbstu.timofeev.utils.FileParser;
import ru.spbstu.timofeev.utils.Pair;
import ru.spbstu.timofeev.utils.PipelineBaseGrammar;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Config {

    private final HashMap<String, String> fields;

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

    public static Pair<Config, RC> fromFile(String filename, PipelineBaseGrammar grammar,
                                            BaseSemantics semantics, Logger logger) {

        Pair<HashMap<String, String>, RC> res = FileParser.readMap(filename, grammar, logger);
        if (res.first == null) {
            logger.severe("Failed to read a map from " + filename);
            return new Pair<>(null, res.second);
        }

        if (!validateSemantics(res.first, semantics, logger)) {
            logger.warning("Semantic validation failed.");
            return new Pair<>(null, RC.CODE_CONFIG_SEMANTIC_ERROR);
        }

        Config cfg = new Config(res.first, logger);

        return new Pair<>(cfg, RC.CODE_SUCCESS);
    }

    private static boolean validateSemantics(HashMap<String, String> cfgMap, BaseSemantics semantics, Logger logger) {
        for (Map.Entry<String, String> entry : cfgMap.entrySet()) {
            if (!semantics.validateField(entry.getKey(), entry.getValue())) {
                logger.warning("Invalid config record: " + entry.getKey() + " -> " + entry.getValue());
                return false;
            }
        }
        return true;
    }
}