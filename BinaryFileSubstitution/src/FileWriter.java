import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IExecutor;
import ru.spbstu.pipeline.IWriter;
import ru.spbstu.pipeline.RetCode;

import java.io.FileOutputStream;
import java.io.IOException;

public class FileWriter implements IWriter {
    private FileOutputStream stream;

    private IExecutable producer;
    private IExecutable consumer;

    // private int bufferSize;

    @Override
    public RetCode.SetterCode setOutputStream(FileOutputStream fileOutputStream) {
        if (fileOutputStream == null) {
            return RetCode.SetterCode.CODE_INVALID_ARGUMENT;
        }
        stream = fileOutputStream;
        return RetCode.SetterCode.CODE_SUCCESS;
    }

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
        /*
        Integer bufferSize = cfg.getIntParameter(GlobalConstants.BUFFER_SIZE_FIELD);

        if (bufferSize == null) {
            return -2;
        }

        this.bufferSize = bufferSize;
        */
        return RetCode.ConfigCode.CODE_SUCCESS;
    }

    @Override
    public RetCode.AlgorithmCode execute(byte[] data) {

        if (data == null) {
            return RetCode.AlgorithmCode.CODE_INVALID_ARGUMENT;
        }

        if (stream == null) {
            return RetCode.AlgorithmCode.CODE_WRITING_ERROR;
        }

        try {
            stream.write(data);
        }
        catch (IOException ex) {
            return RetCode.AlgorithmCode.CODE_WRITING_ERROR;
        }

        return RetCode.AlgorithmCode.CODE_SUCCESS;
    }
}