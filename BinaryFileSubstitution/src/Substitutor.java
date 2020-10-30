import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IExecutor;
import ru.spbstu.pipeline.RC;

import java.util.HashMap;
import java.util.logging.Logger;

public class Substitutor implements IExecutor {

    SubstitutionTable table;
    IExecutable producer;
    IExecutable consumer;

    private Logger logger;

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

    private SemanticConfigValidator getSemanticCfgValidator() {
        HashMap<String, SemanticConfigValidator.ConfigFieldType> svMap;
        svMap = new HashMap<>();
        svMap.put(GlobalConstants.TABLE_FILE_FIELD, SemanticConfigValidator.ConfigFieldType.FT_EXISTING_FILE);
        return new SemanticConfigValidator(svMap, logger);
    }

    @Override
    public RC setConfig(String configFileName) {
        Pair<Config, RC> res = Config.fromFile(configFileName, GlobalConstants.CONFIG_DELIMITER, logger);
        if (res.first == null) {
            logger.severe("Failed to read substitutor config");
            return res.second;
        }

        Config cfg = res.first;

        String tableFilename = cfg.getParameter(GlobalConstants.TABLE_FILE_FIELD);
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

        byte[] outputBytes = new byte[data.length];

        RC retCode = table.Substitute(data, outputBytes);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Substitution error");
            return retCode;
        }

        retCode = consumer.execute(outputBytes);

        if(retCode != RC.CODE_SUCCESS) {
            logger.severe("Substitutor consumer execution error");
        }

        return retCode;
    }
}
