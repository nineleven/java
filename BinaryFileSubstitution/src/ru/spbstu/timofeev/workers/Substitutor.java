package ru.spbstu.timofeev.workers;

import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IExecutor;
import ru.spbstu.pipeline.RC;
import ru.spbstu.timofeev.config.BaseSemantics;
import ru.spbstu.timofeev.config.Config;
import ru.spbstu.timofeev.utils.Pair;
import ru.spbstu.timofeev.utils.PipelineBaseGrammar;

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

    SubstitutionTable table;
    IExecutable producer;
    IExecutable consumer;

    private final Logger logger;

    public Substitutor(Logger logger) {
        this.logger = logger;
    }

    @Override
    public RC setConsumer(IExecutable newConsumer) {
        if (newConsumer == null) {
            logger.warning("Invalid consumer passed to substitutor");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IExecutable newProducer) {
        if (newProducer == null) {
            logger.warning("Invalid producer passed to substitutor");
            return RC.CODE_INVALID_ARGUMENT;
        }

        producer = newProducer;

        return RC.CODE_SUCCESS;
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

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC execute(byte[] data) {

        if (data == null) {
            logger.severe("Invalid substitutor input");
            return RC.CODE_INVALID_ARGUMENT;
        }

        RC retCode = table.Substitute(data);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Substitution error");
            return retCode;
        }

        retCode = consumer.execute(data);

        if(retCode != RC.CODE_SUCCESS) {
            logger.severe("ru.spbstu.timofeev.workers.Substitutor consumer execution error");
        }

        return retCode;
    }
}