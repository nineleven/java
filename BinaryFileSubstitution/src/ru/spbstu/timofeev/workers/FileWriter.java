package ru.spbstu.timofeev.workers;

import ru.spbstu.pipeline.*;
import ru.spbstu.timofeev.config.BaseSemantics;
import ru.spbstu.timofeev.config.Config;
import ru.spbstu.timofeev.utils.Buffer;
import ru.spbstu.timofeev.utils.Pair;
import ru.spbstu.timofeev.utils.PipelineBaseGrammar;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

class WriterGrammar extends PipelineBaseGrammar {

    private static final String[] tokens;

    static {
        WriterSemantics.Fields[] fValues = WriterSemantics.Fields.values();

        tokens = new String[fValues.length];

        for (int i = 0; i < fValues.length; ++i) {
            tokens[i] = fValues[i].toString();
        }
    }

    public WriterGrammar() {
        super(tokens);
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

class WriterSemantics extends BaseSemantics {

    public WriterSemantics(Logger logger) {
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

public class FileWriter implements IWriter {
    private FileOutputStream stream;

    IMediator producerMediator;
    TYPE producerMediatorType;

    private Buffer outputBuffer;

    private final Logger logger;

    private int bufferSize;

    private final TYPE[] inputTypes = {TYPE.BYTE, TYPE.SHORT};

    public FileWriter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public RC setOutputStream(FileOutputStream fileOutputStream) {
        if (fileOutputStream == null) {
            logger.warning("Invalid output stream passed to writer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        stream = fileOutputStream;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer newProducer) {
        if (newProducer == null) {
            logger.warning("Invalid producer passed to writer");
            return RC.CODE_INVALID_ARGUMENT;
        }

        TYPE[] producerTypes = newProducer.getOutputTypes();

        for (TYPE inputType : inputTypes) {
            for (TYPE producerType : producerTypes) {
                if (inputType == producerType) {
                    producerMediator = newProducer.getMediator(inputType);
                    producerMediatorType = inputType;
                    return RC.CODE_SUCCESS;
                }
            }
        }

        logger.warning("Writer unable to find a common type with producer");
        return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
    }

    @Override
    public RC setConsumer(IConsumer newConsumer) {
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConfig(String configFileName) {
        Pair<Config, RC> res = Config.fromFile(configFileName, new WriterGrammar(), new WriterSemantics(logger), logger);
        if (res.first == null) {
            logger.warning("Failed to read writer config from " + configFileName);
            return res.second;
        }

        Config cfg = res.first;

        Integer bufferSize = cfg.getIntParameter(WriterGrammar.Fields.BUFFER_SIZE.toString());
        assert bufferSize != null;

        this.bufferSize = bufferSize;
        this.outputBuffer = new Buffer(bufferSize);

        return RC.CODE_SUCCESS;
    }

    private byte[] convertToByte(Object data) {
        assert (data != null);

        switch (producerMediatorType) {
            case BYTE:
                return (byte[]) data;
            case SHORT:
                short[] shortRepr = (short[]) data;
                byte[] byteRepr = new byte[2 * shortRepr.length];
                ByteBuffer.wrap(byteRepr).asShortBuffer().put(shortRepr);
                return byteRepr;
            default:
                logger.warning("Conversion from type " + producerMediatorType + " is not supported");
                return null;
        }
    }

    @Override
    public RC execute() {

        Object data = producerMediator.getData();

        if (data == null) {
            return RC.CODE_SUCCESS;
        }

        byte[] byteRepr = convertToByte(data);
        if (byteRepr == null) {
            logger.warning("Invalid data passed to Writer");
            return RC.CODE_INVALID_ARGUMENT;
        }

        assert (byteRepr.length <= outputBuffer.capacity());
        outputBuffer.put(byteRepr);

        if (stream == null) {
            logger.severe("Invalid output stream");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }

        try {
            byte[] bufferBytes = outputBuffer.take();
            for(int i = 0; i < bufferBytes.length - bufferSize; i+=bufferSize) {
                stream.write(bufferBytes, i, bufferSize);
            }
            int lastChunkSize = bufferBytes.length % bufferSize == 0 ? bufferSize : bufferBytes.length % bufferSize;
            stream.write(bufferBytes, bufferBytes.length - lastChunkSize, lastChunkSize);
        }
        catch (IOException ex) {
            logger.severe("IO exception while writing");
            return RC.CODE_FAILED_TO_WRITE;
        }

        return RC.CODE_SUCCESS;
    }
}