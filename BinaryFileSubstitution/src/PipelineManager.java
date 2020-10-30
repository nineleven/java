import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

public class PipelineManager implements IConfigurable {

    private String inputFileName;
    private String outputFileName;
    private String inputConfigFileName;
    private String outputConfigFileName;
    private String substitutorConfigFileName;

    private Logger logger;

    private PipelineManager(Logger logger) {
        this.logger = logger;
    }

    public static PipelineManager createInstance(String configFileName, Logger logger) {
        PipelineManager instance = new PipelineManager(logger);
        RC retCode = instance.setConfig(configFileName);
        if (retCode != RC.CODE_SUCCESS) {
            return null;
        }
        return instance;
    }

    public RC setConfig(String configFileName) {
        Pair<Config, RC> res = Config.fromFile(configFileName, GlobalConstants.CONFIG_DELIMITER, logger);
        if (res.first == null) {
            return res.second;
        }

        Config cfg = res.first;

        SemanticConfigValidator validator = getSemanticConfigValidator();
        if (!validator.validate(cfg)) {
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }

        setFieldsFromConfig(cfg);

        return RC.CODE_SUCCESS;
    }

    public RC run() {

        FileReader reader = new FileReader(logger);
        RC retCode = reader.setConfig(inputConfigFileName);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to construct reader config");
            return retCode;
        }

        FileWriter writer = new FileWriter(logger);
        retCode = writer.setConfig(outputConfigFileName);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to construct writer config");
            return retCode;
        }

        Substitutor substitutor = new Substitutor(logger);
        retCode = substitutor.setConfig(substitutorConfigFileName);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to construct substitutor config");
            return retCode;
        }

        FileInputStream inputStream;
        FileOutputStream outputStream;
        try {
            inputStream = new FileInputStream(inputFileName);
        }
        catch (FileNotFoundException ex) {
            logger.severe("Failed to open file " + inputFileName);
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        try {
            outputStream = new FileOutputStream(outputFileName);
        }
        catch (FileNotFoundException ex) {
            logger.severe("Failed to open file " + outputFileName);
            try {
                inputStream.close();
            } catch(IOException ex1) {}
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }

        reader.setConsumer(substitutor);
        reader.setInputStream(inputStream);
        substitutor.setProducer(reader);
        substitutor.setConsumer(writer);
        writer.setProducer(substitutor);
        writer.setOutputStream(outputStream);

        retCode = reader.execute(null);

        try {
            inputStream.close();
        }
        catch (IOException ex) {
            logger.warning("Failed to close input stream");
        }

        try {
            outputStream.close();
        }
        catch (IOException ex) {
            logger.warning("Failed to close output stream");
        }

        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to execute pipeline");
            return retCode;
        }

        return RC.CODE_SUCCESS;
    }

    private SemanticConfigValidator getSemanticConfigValidator() {
        HashMap<String, SemanticConfigValidator.ConfigFieldType> svMap;
        svMap = new HashMap<>();
        svMap.put(GlobalConstants.INPUT_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);
        svMap.put(GlobalConstants.INPUT_CONFIG_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);
        svMap.put(GlobalConstants.OUTPUT_CONFIG_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);
        svMap.put(GlobalConstants.SUBSTITUTOR_CONFIG_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);
        return new SemanticConfigValidator(svMap, logger);
    }

    private void setFieldsFromConfig(Config cfg) {
        String inputFileName = cfg.getParameter(GlobalConstants.INPUT_FILE_FIELD);
        String outputFileName = cfg.getParameter(GlobalConstants.OUTPUT_FILE_FIELD);
        String inputConfigFileName = cfg.getParameter(GlobalConstants.INPUT_CONFIG_FILE_FIELD);
        String outputConfigFileName = cfg.getParameter(GlobalConstants.OUTPUT_CONFIG_FILE_FIELD);
        String substitutorConfigFileName = cfg.getParameter(GlobalConstants.SUBSTITUTOR_CONFIG_FILE_FIELD);

        /*
        We know that these fields should be non-null,
        since semantic validator ensures that
        */
        assert (inputFileName != null && outputFileName != null &&
                inputConfigFileName != null && outputConfigFileName != null);

        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
        this.inputConfigFileName = inputConfigFileName;
        this.outputConfigFileName = outputConfigFileName;
        this.substitutorConfigFileName = substitutorConfigFileName;
    }
}