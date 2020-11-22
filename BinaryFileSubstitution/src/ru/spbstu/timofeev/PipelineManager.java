package ru.spbstu.timofeev;

import ru.spbstu.pipeline.*;
import ru.spbstu.timofeev.config.BaseSemantics;
import ru.spbstu.timofeev.config.Config;
import ru.spbstu.timofeev.utils.Pair;
import ru.spbstu.timofeev.utils.PipelineBaseGrammar;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

class ManagerGrammar extends PipelineBaseGrammar {

    private static final String[] tokens;

    static {
        ManagerSemantics.Fields[] fValues = ManagerSemantics.Fields.values();

        tokens = new String[fValues.length];

        for (int i = 0; i < fValues.length; ++i) {
            tokens[i] = fValues[i].toString();
        }
    }

    public ManagerGrammar() {
        super(tokens);
    }
}

class ManagerSemantics extends BaseSemantics {
    private static final String workersDelimiter = ";";
    private static final String workersInnerDelimiter = ",";

    public ManagerSemantics(Logger logger) {
        super(logger);
    }

    public static String workersDelimiter() {
        return workersDelimiter;
    }

    public static String workersInnerDelimiter() {
        return workersInnerDelimiter;
    }

    @Override
    public boolean validateField(String fieldName, String fieldValue) {
        assert fieldName != null && fieldValue != null;

        if (fieldName.equals(Fields.INPUT_FILE.toString())) {
            if (validateExistingFile(fieldValue)) {
                return true;
            }
            getLogger().warning("Invalid input file: " + fieldValue);
            return false;
        }
        else if (fieldName.equals(Fields.OUTPUT_FILE.toString())) {
            return true;
        }
        else if (fieldName.equals(Fields.PIPELINE_STRUCTURE.toString())) {
            return validatePipelineStructure(fieldValue);
        }
        else {
            getLogger().warning("Unknown field validation queried: " + fieldName);
        }
        return true;
    }

    private boolean validatePipelineStructure(String pStruct) {
        String[] workerStrings = pStruct.split(workersDelimiter());

        for (int workerId = 0; workerId < workerStrings.length; ++workerId) {
            String[] workerParams = workerStrings[workerId].split(ManagerSemantics.workersInnerDelimiter());

            if (workerParams.length != 2) {
                getLogger().warning("Invalid step: " + workerStrings[workerId]);
                return false;
            }

            for (int i = 0; i < workerParams.length; ++i) {
                workerParams[i] = workerParams[i].trim();
            }

            if (!validatePipelineStepClass(workerParams[0]) ||
                    !validateExistingFile(workerParams[1])) {
                getLogger().warning("Invalid step: " + workerStrings[workerId]);
                return false;
            }

            if (workerId == 0 && !validateIReader(workerParams[0]) ||
                    workerId == workerStrings.length - 1 && !validateIWriter(workerParams[0])) {
                getLogger().warning("Invalid step: " + workerStrings[workerId]);
                return false;
            }
        }
        return true;
    }

    private boolean validateIReader(String className) {
        try {
            Class<?> clazz = Class.forName(className);

            if (!IReader.class.isAssignableFrom(clazz)) {
                return false;
            }
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }

    private boolean validateIWriter(String className) {
        try {
            Class<?> clazz = Class.forName(className);

            if (!IWriter.class.isAssignableFrom(clazz)) {
                return false;
            }
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }

    private boolean validatePipelineStepClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            Class<?>[] params = {Logger.class};
            clazz.getConstructor(params);

            if (!(IPipelineStep.class.isAssignableFrom(clazz)) ||
                    !(IConfigurable.class.isAssignableFrom(clazz))) {
                getLogger().warning(className + " is not a valid pipeline step");
                return false;
            }
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            getLogger().warning(className + " is not a valid pipeline step");
            return false;
        }
        return true;
    }

    public enum Fields {
        INPUT_FILE("input_file"),
        OUTPUT_FILE("output_file"),
        PIPELINE_STRUCTURE("pipeline");

        private final String name;

        Fields(String name) {
            this.name = name;
        }

        public String toString() {
            return this.name;
        }
    }
}

public class PipelineManager implements IConfigurable {

    private String inputFileName;
    private String outputFileName;

    private Pair<String ,String>[] workerTemplates;

    private final Logger logger;

    private PipelineManager(Logger logger) {
        this.logger = logger;
    }

    public static PipelineManager createInstance(String configFileName, Logger logger) {
        PipelineManager instance = new PipelineManager(logger);
        RC retCode = instance.setConfig(configFileName);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Failed to set manager config.");
            return null;
        }
        return instance;
    }

    public RC setConfig(String configFileName) {
        Pair<Config, RC> res = Config.fromFile(configFileName, new ManagerGrammar(), new ManagerSemantics(logger), logger);
        if (res.first == null) {
            logger.severe("Failed to read config file " + configFileName);
            return res.second;
        }

        Config cfg = res.first;

        setFieldsFromConfig(cfg);

        return RC.CODE_SUCCESS;
    }

    public RC run() {
        Pair<Pair<FileInputStream, FileOutputStream>, RC> res = openStreams(inputFileName, outputFileName);
        if (res.second != RC.CODE_SUCCESS) {
            logger.severe("Failed to open streams");
            return res.second;
        }

        FileInputStream inputStream = res.first.first;
        FileOutputStream outputStream = res.first.second;

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

    private void setFieldsFromConfig(Config cfg) {
        assert cfg != null;

        String inputFileName = cfg.getParameter(ManagerSemantics.Fields.INPUT_FILE.toString());
        String outputFileName = cfg.getParameter(ManagerSemantics.Fields.OUTPUT_FILE.toString());
        String pStruct = cfg.getParameter(ManagerSemantics.Fields.PIPELINE_STRUCTURE.toString());

        assert (inputFileName != null && outputFileName != null && pStruct != null);

        this.workerTemplates = getWorkerTemplates(pStruct);
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
    }

    private Pair<String, String>[] getWorkerTemplates(String pStruct) {
        assert pStruct != null;

        String[] workerStrings = pStruct.split(ManagerSemantics.workersDelimiter());

        Pair<String, String>[] templates = new Pair[workerStrings.length];

        for (int k = 0; k < workerStrings.length; ++k) {
            String[] workerParams = workerStrings[k].split(ManagerSemantics.workersInnerDelimiter());

            assert workerParams.length == 2;

            for (int i = 0; i < workerParams.length; ++i) {
                workerParams[i] = workerParams[i].trim();
            }

            templates[k] = new Pair<>(workerParams[0], workerParams[1]);
        }

        return templates;
    }

    private Pair<Pair<FileInputStream, FileOutputStream>, RC> openStreams(String inputFileName, String outputFileName) {
        assert  inputFileName != null && outputFileName != null;

        FileInputStream inputStream;
        FileOutputStream outputStream;
        try {
            inputStream = new FileInputStream(inputFileName);
        }
        catch (FileNotFoundException e) {
            return new Pair<>(null, RC.CODE_INVALID_INPUT_STREAM);
        }
        try {
            outputStream = new FileOutputStream(outputFileName);
        }
        catch (FileNotFoundException e) {
            closeStream(inputStream);
            return new Pair<>(null, RC.CODE_INVALID_OUTPUT_STREAM);
        }
        return new Pair<>(new Pair<>(inputStream, outputStream), RC.CODE_SUCCESS);
    }

    private void closeStream(Closeable c) {
        try {
            c.close();
        }
        catch (IOException e) {

        }
    }

    private Pair<IPipelineStep[], RC> createWorkers(FileInputStream inputStream, FileOutputStream outputStream) {
        assert inputStream != null;
        assert outputStream != null;

        IPipelineStep[] steps = new IPipelineStep[workerTemplates.length];

        for (int workerId = 0; workerId < workerTemplates.length; ++workerId) {
            IPipelineStep worker = createWorker(workerTemplates[workerId].first);

            assert worker != null;
            assert IConfigurable.class.isAssignableFrom(worker.getClass());

            RC rc = ((IConfigurable)worker).setConfig(workerTemplates[workerId].second);
            if (rc != RC.CODE_SUCCESS) {
                return new Pair<>(null, rc);
            }

            steps[workerId] = worker;
        }

        assert IReader.class.isAssignableFrom(steps[0].getClass());
        assert IWriter.class.isAssignableFrom(steps[steps.length-1].getClass());

        ((IReader)steps[0]).setInputStream(inputStream);
        ((IWriter)steps[steps.length-1]).setOutputStream(outputStream);

        return new Pair<>(steps, RC.CODE_SUCCESS);
    }

    private IPipelineStep createWorker(String className) {
        assert className != null;

        IPipelineStep step;
        try {
            Class<?> clazz = Class.forName(className);
            Class<?>[] params = {Logger.class};

            assert IPipelineStep.class.isAssignableFrom(clazz);

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
        assert workers != null;

        for(int i = 0; i < workers.length - 1; ++i) {
            workers[i].setConsumer(workers[i+1]);
            workers[i+1].setProducer(workers[i]);
        }
    }
}