package ru.spbstu.timofeev.workers;

import ru.spbstu.pipeline.*;
import ru.spbstu.timofeev.config.BaseSemantics;
import ru.spbstu.timofeev.config.Config;
import ru.spbstu.timofeev.utils.Buffer;
import ru.spbstu.timofeev.utils.Pair;
import ru.spbstu.timofeev.utils.PipelineBaseGrammar;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Logger;

class ReaderGrammar extends PipelineBaseGrammar {

    private static final String[] tokens;

    static {
        ReaderSemantics.Fields[] fValues = ReaderSemantics.Fields.values();

        tokens = new String[fValues.length];

        for (int i = 0; i < fValues.length; ++i) {
            tokens[i] = fValues[i].toString();
        }
    }

    public ReaderGrammar() {
        super(tokens);
    }
}

class ReaderSemantics extends BaseSemantics {

    public ReaderSemantics(Logger logger) {
        super(logger);
    }

    @Override
    public boolean validateField(String fieldName, String fieldValue) {
        if (fieldName.equals(Fields.BUFFER_SIZE.toString())) {
            return BaseSemantics.validatePositiveInt(fieldValue);
        }
        else {
            getLogger().warning("Unknown field validation queried: " + fieldName);
        }
        return true;
    }

    public enum Fields {
        BUFFER_SIZE("buffer_size");

        private final String name;

        Fields(String name) {
            this.name = name;
        }

        public String toString() {
            return this.name;
        }
    }
}

public class FileReader implements IReader {

    private FileInputStream stream;

    private IConsumer consumer;

    private Buffer outputBuffer;

    private final Logger logger;

    private int bufferSize;

    private final TYPE[] outputTypes = {TYPE.BYTE, TYPE.SHORT};

    private boolean finishing;

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
    public RC setConsumer(IConsumer newConsumer) {
        if (newConsumer == null) {
            logger.warning("Invalid consumer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer newProducer) {
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConfig(String configFileName) {

        Pair<Config, RC> res = Config.fromFile(configFileName, new ReaderGrammar(), new ReaderSemantics(logger), logger);
        if (res.first == null) {
            logger.severe("Failed to read reader config from " + configFileName);
            return res.second;
        }

        Config cfg = res.first;

        Integer bufferSize = cfg.getIntParameter(ReaderSemantics.Fields.BUFFER_SIZE.toString());
        assert bufferSize != null;

        this.bufferSize = bufferSize;
        this.outputBuffer = new Buffer(bufferSize);

        return RC.CODE_SUCCESS;
    }

    @Override
    public TYPE[] getOutputTypes() {
        return outputTypes;
    }

    @Override
    public IMediator getMediator(TYPE type) {
        switch (type) {
            case BYTE:
                return new ByteMediator();
            case SHORT:
                return new ShortMediator();
            default:
                logger.warning("Mediator of type " + type + " is not implemented");
                return null;
        }
    }

    class ByteMediator implements IMediator {
        @Override
        public Object getData() {
            byte[] data = outputBuffer.take();
            if (data.length == 0 && finishing) {
                return null;
            }
            return data;
        }
    }

    class ShortMediator implements IMediator {
        @Override
        public Object getData() {
            short[] data = outputBuffer.takeShort();
            if (data.length == 0 && finishing) {
                return null;
            }
            return data;
        }
    }

    private int readBytePortion(byte[] buffer, int bufferSize) {
        int bytesRead;
        try {
            bytesRead = stream.read(buffer, 0, bufferSize);
        } catch (IOException ex) {
            logger.severe("IO exception while reading");
            return -1;
        }

        if (bytesRead < 0) {
            return 0;
        }

        return bytesRead;
    }

    @Override
    public RC execute() {
        if (stream == null) {
            logger.severe("Invalid input stream");
            return RC.CODE_INVALID_INPUT_STREAM;
        }

        byte[] buffer = new byte[bufferSize];
        int bytesRead;

        finishing = false;

        while(true) {
            bytesRead = readBytePortion(buffer, bufferSize);
            if (bytesRead < 0) {
                logger.severe("FileReader failed to read");
                return RC.CODE_FAILED_TO_READ;
            }
            else if (bytesRead == 0) {
                finishing = true;

                RC retCode = consumer.execute();
                if (retCode != RC.CODE_SUCCESS) {
                    logger.severe("Reader consumer execution error");
                    return retCode;
                }

                break;
            }

            assert (bytesRead <= outputBuffer.capacity());
            outputBuffer.put(buffer, 0, bytesRead);

            while(!outputBuffer.isEmpty()) {
                RC retCode = consumer.execute();
                if (retCode != RC.CODE_SUCCESS) {
                    logger.severe("Reader consumer execution error");
                    return retCode;
                }
            }
        }

        return RC.CODE_SUCCESS;
    }
}

