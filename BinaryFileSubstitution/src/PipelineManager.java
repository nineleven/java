import ru.spbstu.pipeline.*;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.logging.Logger;

public class PipelineManager implements IConfigurable {

    private String inputFileName;
    private String outputFileName;

    private String[] workerConfigFilenames;
    private String[] workerNames;

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

        if (!getSemanticConfigValidator().validate(cfg)) {
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }

        setFieldsFromConfig(cfg);

        return RC.CODE_SUCCESS;
    }

    public RC run() {

        FileInputStream inputStream;
        FileOutputStream outputStream;
        try {
            inputStream = new FileInputStream(inputFileName);
        }
        catch (FileNotFoundException e) {
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        try {
            outputStream = new FileOutputStream(outputFileName);
        }
        catch (FileNotFoundException e) {
            closeStream(inputStream);
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }

        Pair<IPipelineStep[], RC> resWorkers = createWorkers(inputStream, outputStream);
        if (resWorkers.first == null) {
            closeStream(inputStream);
            closeStream(outputStream);
            logger.severe("Failed to create workers");
            return resWorkers.second;
        }

        IPipelineStep[] workers = resWorkers.first;

        putWorkersInChain(workers);

        RC retCode = workers[0].execute(null);

        closeStream(inputStream);
        closeStream(outputStream);

        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to execute pipeline");
        }

        return retCode;
    }

    /*
    ПРОВЕРКИИИ
     */
    private Pair<IPipelineStep[], RC> createWorkers(FileInputStream inputStream, FileOutputStream outputStream) {
        IPipelineStep[] steps = new IPipelineStep[workerNames.length];

        for (int workerId = 0; workerId < workerNames.length; ++workerId) {
            steps[workerId] = createWorker(workerNames[workerId]);
            ((IConfigurable)steps[workerId]).setConfig(workerConfigFilenames[workerId]);
        }

        ((IReader)steps[0]).setInputStream(inputStream);
        ((IWriter)steps[steps.length-1]).setOutputStream(outputStream);

        return new Pair<>(steps, RC.CODE_SUCCESS);
    }

    private IPipelineStep createWorker(String className) {
        IPipelineStep step;
        try {
            Class clazz = Class.forName(className);
            Class[] params = {Logger.class};
            step = (IPipelineStep) clazz.getConstructor(params).newInstance(logger);
        } catch (ClassNotFoundException | NoSuchMethodException |
                InvocationTargetException | IllegalAccessException |
                InstantiationException e) {
            logger.warning("Failed to instantiate a " + className + " class");
            return null;
        }
        return step;
    }

    private void putWorkersInChain(IPipelineStep[] workers) {
        assert workers != null && workers.length > 0;

        assert workers[0] != null;

        for(int i = 0; i < workers.length - 1; ++i) {
            assert workers[i+1] != null;

            workers[i].setConsumer(workers[i+1]);
            workers[i+1].setProducer(workers[i]);
        }
    }

    private void closeStream(Closeable c) {
        try {
            c.close();
        }
        catch (IOException e) {

        }
    }

    private SemanticConfigValidator getSemanticConfigValidator() {
        HashMap<String, SemanticConfigValidator.ConfigFieldType> svMap = new HashMap<>();

        svMap.put(GlobalConstants.INPUT_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);
        svMap.put(GlobalConstants.OUTPUT_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_IS_PRESENT);

        svMap.put(GlobalConstants.PIPELINE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_PIPELINE);

        return new SemanticConfigValidator(svMap, logger);
    }

    private void setWorkers(String pipeline) {
        String[] workers = pipeline.split(";");

        workerNames = new String[workers.length];
        workerConfigFilenames = new String[workers.length];

        for (int k = 0; k < workers.length; ++k) {
            String[] workerParams = workers[k].split(",");

            assert workerParams.length == 2;

            for (int i = 0; i < workerParams.length; ++i) {
                workerParams[i] = workerParams[i].trim();
            }

            workerNames[k] = workerParams[0];
            workerConfigFilenames[k] = workerParams[1];
        }
    }

    private void setFieldsFromConfig(Config cfg) {
        String inputFileName = cfg.getParameter(GlobalConstants.INPUT_FILE_FIELD);
        String outputFileName = cfg.getParameter(GlobalConstants.OUTPUT_FILE_FIELD);

        String pipeline = cfg.getParameter(GlobalConstants.PIPELINE_FIELD);

        /*
        We know that these fields should be non-null,
        since semantic validator ensures that.
        */
        assert (inputFileName != null && outputFileName != null &&
                pipeline != null);

        setWorkers(pipeline);

        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
    }
}