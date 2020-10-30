import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IWriter;
import ru.spbstu.pipeline.RC;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

public class FileWriter implements IWriter {
    private FileOutputStream stream;

    private IExecutable producer;
    private IExecutable consumer;

    private final Logger logger;

    private int bufferSize;

    public FileWriter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public RC setOutputStream(FileOutputStream fileOutputStream) {
        if (fileOutputStream == null) {
            logger.warning("An invalid output stream passed to writer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        stream = fileOutputStream;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConsumer(IExecutable newConsumer) {
        consumer = newConsumer;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IExecutable newProducer) {
        if (newProducer == null) {
            logger.warning("Invalid producer passed to writer");
            return RC.CODE_INVALID_ARGUMENT;
        }

        producer = newProducer;

        return RC.CODE_SUCCESS;
    }

    private SemanticConfigValidator getSemanticCfgValidator() {
        HashMap<String, SemanticConfigValidator.ConfigFieldType> svMap;
        svMap = new HashMap<>();
        svMap.put(GlobalConstants.BUFFER_SIZE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_INT);
        return new SemanticConfigValidator(svMap, logger);
    }

    @Override
    public RC setConfig(String configFileName) {
        Pair<Config, RC> res = Config.fromFile(configFileName, GlobalConstants.CONFIG_DELIMITER, logger);
        if (res.first == null) {
            logger.warning("Failed to read writer config from " + configFileName);
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

    @Override
    public RC execute(byte[] data) {

        /*
        ADD BUFFERED OUTPUT
         */

        if (data == null) {
            logger.severe("Invalid writer input");
            return RC.CODE_INVALID_ARGUMENT;
        }

        if (stream == null) {
            logger.severe("Invalid output stream");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }

        try {
            stream.write(data);
        }
        catch (IOException ex) {
            logger.severe("IO exception while writing");
            return RC.CODE_FAILED_TO_WRITE;
        }

        return RC.CODE_SUCCESS;
    }
}