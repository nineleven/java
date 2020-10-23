import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IExecutor;
import ru.spbstu.pipeline.RetCode;

public class Substitutor implements IExecutor {

    SubstitutionTable table;
    IExecutable producer;
    IExecutable consumer;

    @Override
    public RetCode.SetterCode setConsumer(IExecutable newConsumer) {
        if (newConsumer == null) {
            return RetCode.SetterCode.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;

        return RetCode.SetterCode.CODE_SUCCESS;
    }

    @Override
    public RetCode.SetterCode setProducer(IExecutable newProducer) {
        if (newProducer == null) {
            return RetCode.SetterCode.CODE_INVALID_ARGUMENT;
        }

        producer = newProducer;

        return RetCode.SetterCode.CODE_SUCCESS;
    }

    @Override
    public RetCode.ConfigCode setConfig(String s) {
        Config cfg = Config.fromFile(s, GlobalConstants.CONFIG_DELIMITER);
        if (cfg == null) {
            return RetCode.ConfigCode.CODE_FAILED_TO_READ;
        }

        String tableFilename = cfg.getParameter(GlobalConstants.TABLE_FILE_FIELD);
        if (tableFilename == null) {
            return RetCode.ConfigCode.CODE_MISSING_PARAMETER;
        }

        table = SubstitutionTable.fromFile(tableFilename);
        if (table == null) {
            return RetCode.ConfigCode.CODE_BAD_PARAMETER;
        }

        return RetCode.ConfigCode.CODE_SUCCESS;
    }

    @Override
    public RetCode.AlgorithmCode execute(byte[] data) {

        if (data == null) {
            return RetCode.AlgorithmCode.CODE_INVALID_ARGUMENT;
        }

        byte[] outputBytes = new byte[data.length];

        RetCode.AlgorithmCode retCode = table.Substitute(data, outputBytes);
        if (retCode != RetCode.AlgorithmCode.CODE_SUCCESS) {
            return retCode;
        }

        return consumer.execute(outputBytes);
    }
}
