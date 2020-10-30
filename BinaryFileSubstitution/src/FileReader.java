import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IReader;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

public class FileReader implements IReader {

    private FileInputStream stream;

    private IExecutable producer;
    private IExecutable consumer;

    private final Logger logger;

    private int bufferSize;

    public FileReader(Logger logger) {
        this.logger = logger;
    }

    @Override
    public RC setInputStream(FileInputStream fileInputStream) {
        if (fileInputStream == null) {
            logger.warning("Invalid input stream");
            return RC.CODE_INVALID_ARGUMENT;
        }
        stream = fileInputStream;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConsumer(IExecutable newConsumer) {
        if (newConsumer == null) {
            logger.warning("Invalid consumer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IExecutable newProducer) {
        producer = newProducer;

        return RC.CODE_SUCCESS;
    }

    private SemanticConfigValidator getSemanticCfgValidator() {
        HashMap<String, SemanticConfigValidator.ConfigFieldType> svMap;
        svMap = new HashMap<>();
        svMap.put(GlobalConstants.BUFFER_SIZE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_POSITIVE_INT);
        return new SemanticConfigValidator(svMap, logger);
    }

    @Override
    public RC setConfig(String configFileName) {

        Pair<Config, RC> res = Config.fromFile(configFileName, GlobalConstants.CONFIG_DELIMITER, logger);
        if (res.first == null) {
            logger.severe("Failed to read reader config from " + configFileName);
            return res.second;
        }

        Config cfg = res.first;

        if (!getSemanticCfgValidator().validate(cfg)) {
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }

        Integer bufferSize = cfg.getIntParameter(GlobalConstants.BUFFER_SIZE_FIELD);
        assert bufferSize != null;

        this.bufferSize = bufferSize;

        return RC.CODE_SUCCESS;
    }

    private int readBytePortion(byte[] buffer, int bufferSize) {
        int bytesRead;
        try {
            bytesRead = stream.read(buffer, 0, bufferSize);
        } catch (IOException ex) {
            logger.severe("IO exception while reading");
            return -1;
        }
        return bytesRead;
    }

    @Override
    public RC execute(byte[] data) {
        if (stream == null) {
            logger.severe("Invalid input stream");
            return RC.CODE_INVALID_INPUT_STREAM;
        }

        byte[] buffer = new byte[bufferSize];

        int bytesRead;

        while(true) {
            bytesRead = readBytePortion(buffer, bufferSize);
            if (bytesRead < 1) {
                break;
            }

            byte[] output = new byte[bytesRead];
            System.arraycopy(buffer, 0, output, 0, bytesRead);

            RC retCode = consumer.execute(output);
            if (retCode != RC.CODE_SUCCESS) {
                logger.severe("Reader consumer execution error");
                return retCode;
            }
        }

        return RC.CODE_SUCCESS;
    }
}

