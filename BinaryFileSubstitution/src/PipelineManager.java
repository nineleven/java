import ru.spbstu.pipeline.*;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.logging.Logger;

public class PipelineManager implements IConfigurable {

    private String inputFileName;
    private String outputFileName;

    private String inputConfigFileName;
    private String outputConfigFileName;
    private String substitutorConfigFileName;

    private String readerClassName, writerClassName, executorClassName;

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
    Пока 3 фиксированных работника reader, executor, writer
     */
    private Pair<IPipelineStep[], RC> createWorkers(FileInputStream inputStream, FileOutputStream outputStream) {
        Pair<IReader, RC> readerRes = createReader(inputStream);
        if (readerRes.first == null) {
            logger.severe("Failed to create a reader");
            return new Pair<>(null, readerRes.second);
        }
        IReader reader = readerRes.first;

        Pair<IWriter, RC> writerRes = createWriter(outputStream);
        if (writerRes.first == null) {
            logger.severe("Failed to create a reader");
            return new Pair<>(null, writerRes.second);
        }
        IWriter writer = writerRes.first;

        Pair<IExecutor, RC> executorRes = createExecutor();
        if (executorRes.first == null) {
            logger.severe("Failed to create a reader");
            return new Pair<>(null, executorRes.second);
        }
        IExecutor executor = executorRes.first;

        IPipelineStep[] workers = new IPipelineStep[] {reader, executor, writer};

        return new Pair<>(workers, RC.CODE_SUCCESS);
    }

    private IPipelineStep createPipelineStep(String className) {
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

    private Pair<IReader, RC> createReader(FileInputStream inputStream) {
        IReader reader = (IReader) createPipelineStep(readerClassName);
        if (reader == null) {
            logger.severe("Failed to create a reader");
            return new Pair<>(null, RC.CODE_FAILED_PIPELINE_CONSTRUCTION);
        }
        RC retCode = reader.setConfig(inputConfigFileName);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to construct a reader config");
            return new Pair<>(null, retCode);
        }

        reader.setInputStream(inputStream);

        return new Pair<>(reader, RC.CODE_SUCCESS);
    }

    private Pair<IWriter, RC> createWriter(FileOutputStream outputStream) {
        IWriter writer = (IWriter) createPipelineStep(writerClassName);
        if (writer == null) {
            logger.severe("Failed to create a writer");
            return new Pair<>(null, RC.CODE_FAILED_PIPELINE_CONSTRUCTION);
        }
        RC retCode = writer.setConfig(outputConfigFileName);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to construct a writer config");
            return new Pair<>(null, retCode);
        }

        writer.setOutputStream(outputStream);

        return new Pair<>(writer, RC.CODE_SUCCESS);
    }

    private Pair<IExecutor, RC> createExecutor() {
        IExecutor executor = (IExecutor) createPipelineStep(executorClassName);
        if (executor == null) {
            logger.severe("Failed to create an executor");
            return new Pair<>(null, RC.CODE_FAILED_PIPELINE_CONSTRUCTION);
        }
        RC retCode = executor.setConfig(substitutorConfigFileName);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to construct an executor config");
            return new Pair<>(null, retCode);
        }
        return new Pair<>(executor, RC.CODE_SUCCESS);
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

        svMap.put(GlobalConstants.INPUT_CONFIG_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);
        svMap.put(GlobalConstants.OUTPUT_CONFIG_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);
        svMap.put(GlobalConstants.EXECUTOR_CONFIG_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);

        svMap.put(GlobalConstants.READER_CLASSNAME_FIELD, SemanticConfigValidator.ConfigFieldType.FT_CLASS_NAME);
        svMap.put(GlobalConstants.WRITER_CLASSNAME_FIELD, SemanticConfigValidator.ConfigFieldType.FT_CLASS_NAME);
        svMap.put(GlobalConstants.EXECUTOR_CLASSNAME_FIELD, SemanticConfigValidator.ConfigFieldType.FT_CLASS_NAME);

        return new SemanticConfigValidator(svMap, logger);
    }

    private void setFieldsFromConfig(Config cfg) {
        String inputFileName = cfg.getParameter(GlobalConstants.INPUT_FILE_FIELD);
        String outputFileName = cfg.getParameter(GlobalConstants.OUTPUT_FILE_FIELD);

        String inputConfigFileName = cfg.getParameter(GlobalConstants.INPUT_CONFIG_FILE_FIELD);
        String outputConfigFileName = cfg.getParameter(GlobalConstants.OUTPUT_CONFIG_FILE_FIELD);
        String substitutorConfigFileName = cfg.getParameter(GlobalConstants.EXECUTOR_CONFIG_FILE_FIELD);

        String readerClassName = cfg.getParameter(GlobalConstants.READER_CLASSNAME_FIELD);
        String writerClassName = cfg.getParameter(GlobalConstants.WRITER_CLASSNAME_FIELD);
        String executorClassName = cfg.getParameter(GlobalConstants.EXECUTOR_CLASSNAME_FIELD);

        /*
        We know that these fields should be non-null,
        since semantic validator ensures that.
        */
        assert (inputFileName != null && outputFileName != null &&
                inputConfigFileName != null && outputConfigFileName != null &&
                readerClassName != null && writerClassName != null &&
                executorClassName != null);

        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;

        this.inputConfigFileName = inputConfigFileName;
        this.outputConfigFileName = outputConfigFileName;
        this.substitutorConfigFileName = substitutorConfigFileName;

        this.readerClassName = readerClassName;
        this.writerClassName = writerClassName;
        this.executorClassName = executorClassName;
    }
}