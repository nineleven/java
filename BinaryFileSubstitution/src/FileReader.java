import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IExecutor;
import ru.spbstu.pipeline.IReader;
import ru.spbstu.pipeline.RetCode;

import java.io.FileInputStream;
import java.io.IOException;

public class FileReader implements IReader {

    private FileInputStream stream;

    private IExecutable producer;
    private IExecutable consumer;

    private int bufferSize;

    @Override
    public RetCode.SetterCode setInputStream(FileInputStream fileInputStream) {
        if (fileInputStream == null) {
            return RetCode.SetterCode.CODE_INVALID_ARGUMENT;
        }
        stream = fileInputStream;
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
    public RetCode.ConfigCode setConfig(String configFileName) {
        Config cfg = Config.fromFile(configFileName, GlobalConstants.CONFIG_DELIMITER);
        if (cfg == null) {
            return RetCode.ConfigCode.CODE_FAILED_TO_READ;
        }

        Integer bufferSize = cfg.getIntParameter(GlobalConstants.BUFFER_SIZE_FIELD);
        if (bufferSize == null) {
            return RetCode.ConfigCode.CODE_MISSING_PARAMETER;
        }

        this.bufferSize = bufferSize;

        return RetCode.ConfigCode.CODE_SUCCESS;
    }

    private int readBytePortion(byte[] buffer, int bufferSize) {
        int bytesRead;
        try {
            bytesRead = stream.read(buffer, 0, bufferSize);
        } catch (IOException ex) {
            return -1;
        }
        return bytesRead;
    }

    @Override
    public RetCode.AlgorithmCode execute(byte[] data) {
        if (stream == null) {
            return RetCode.AlgorithmCode.CODE_INVALID_ARGUMENT;
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

            RetCode.AlgorithmCode retCode = consumer.execute(output);
            if (retCode != RetCode.AlgorithmCode.CODE_SUCCESS) {
                return retCode;
            }
        }

        return RetCode.AlgorithmCode.CODE_SUCCESS;
    }
}

