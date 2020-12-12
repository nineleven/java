package ru.spbstu.timofeev.workers;

import ru.spbstu.pipeline.*;
import ru.spbstu.timofeev.config.BaseSemantics;
import ru.spbstu.timofeev.config.Config;
import ru.spbstu.timofeev.utils.Buffer;
import ru.spbstu.timofeev.utils.Pair;
import ru.spbstu.timofeev.utils.PipelineBaseGrammar;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

class SubstitutorGrammar extends PipelineBaseGrammar {

    private static final String[] tokens;

    static {
        SubstitutorSemantics.Fields[] fValues = SubstitutorSemantics.Fields.values();

        tokens = new String[fValues.length];

        for (int i = 0; i < fValues.length; ++i) {
            tokens[i] = fValues[i].toString();
        }
    }

    public SubstitutorGrammar() {
        super(tokens);
    }
}

class SubstitutorSemantics extends BaseSemantics {

    public SubstitutorSemantics(Logger logger) {
        super(logger);
    }

    @Override
    public boolean validateField(String fieldName, String fieldValue) {
        if (fieldName.equals(Fields.TABLE_FILE.toString())) {
            return BaseSemantics.validateExistingFile(fieldValue);
        }
        else {
            getLogger().warning("Unknown field validation queried: " + fieldName);
        }
        return true;
    }

    public enum Fields {
        TABLE_FILE("table_file");

        private final String name;

        Fields(String name) {
            this.name = name;
        }

        public String toString() {
            return this.name;
        }
    }
}

public class Substitutor implements IExecutor {

    private final int INITIAL_OUTPUT_BUFFER_CAPACITY = 10;
    private Buffer outputBuffer;

    SubstitutionTable table;

    IMediator producerMediator;
    TYPE producerMediatorType;

    IConsumer consumer;

    final TYPE[] inputTypes = {TYPE.BYTE, TYPE.SHORT};
    final TYPE[] outputTypes = {TYPE.BYTE, TYPE.SHORT};

    private final Logger logger;

    private boolean finishing;

    public Substitutor(Logger logger) {
        this.logger = logger;
        this.finishing = false;
    }

    @Override
    public RC setConsumer(IConsumer newConsumer) {
        if (newConsumer == null) {
            logger.warning("Invalid consumer passed to substitutor");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer newProducer) {
        if (newProducer == null) {
            logger.warning("Invalid producer passed to substitutor");
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

        logger.warning("Substitutor unable to find a common type with producer");
        return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
    }

    @Override
    public RC setConfig(String configFileName) {
        Pair<Config, RC> res = Config.fromFile(configFileName, new SubstitutorGrammar(), new SubstitutorSemantics(logger), logger);
        if (res.first == null) {
            logger.severe("Failed to read substitutor config");
            return res.second;
        }

        Config cfg = res.first;

        String tableFilename = cfg.getParameter(SubstitutorSemantics.Fields.TABLE_FILE.toString());
        assert  tableFilename != null;

        Pair<SubstitutionTable, RC> tableRes = SubstitutionTable.fromFile(tableFilename, logger);
        if (tableRes.first == null) {
            logger.severe("Failed to construct a substitution table from " + tableFilename);
            return tableRes.second;
        }

        this.table = tableRes.first;
        this.outputBuffer = new Buffer(INITIAL_OUTPUT_BUFFER_CAPACITY);

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
                return new Substitutor.ByteMediator();
            case SHORT:
                return new Substitutor.ShortMediator();
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

    private RC processByte(byte[] data) {
        if (table == null) {
            logger.severe("Config is not set");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }

        RC retCode = table.Substitute(data);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Substitution error");
            return retCode;
        }

        outputBuffer.put(data);

        return RC.CODE_SUCCESS;
    }

    private RC processShort(short[] data) {
        if (table == null) {
            logger.severe("Config is not set");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }

        byte[] byteRepr = new byte[2 * data.length];
        ByteBuffer.wrap(byteRepr).asShortBuffer().put(data);

        RC retCode = table.Substitute(byteRepr);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Substitution error");
            return retCode;
        }

        assert (byteRepr.length <= outputBuffer.capacity());
        outputBuffer.put(byteRepr);

        return RC.CODE_SUCCESS;
    }

    private RC processData(Object data) {
        if (data == null) {
            return RC.CODE_SUCCESS;
        }

        RC retCode;

        try {
            switch (producerMediatorType) {
                case BYTE:
                    retCode = processByte((byte[])data);
                    break;
                case SHORT:
                    retCode = processShort((short[])data);
                    break;
                default:
                    logger.warning("Unknown mediator type");
                    retCode = RC.CODE_INVALID_ARGUMENT;
            }
        } catch (ClassCastException ex) {
            logger.severe("Wrong data type passed to substitutor");
            retCode = RC.CODE_INVALID_ARGUMENT;
        }

        return retCode;
    }

    @Override
    public RC execute() {
        Object data = producerMediator.getData();

        RC retCode = processData(data);
        if (retCode != RC.CODE_SUCCESS) {
            logger.warning("Substitutor failed to process data");
            return retCode;
        }

        while(!outputBuffer.isEmpty()) {
            retCode = consumer.execute();
            if (retCode != RC.CODE_SUCCESS) {
                logger.severe("Substitutor consumer execution error");
                return retCode;
            }
        }

        if (data == null) {
            finishing = true;
            consumer.execute();
        }

        return RC.CODE_SUCCESS;
    }
}