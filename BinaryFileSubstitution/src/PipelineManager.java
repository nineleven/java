import ru.spbstu.pipeline.IConfigurable;
import ru.spbstu.pipeline.RetCode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class PipelineManager implements IConfigurable {

    public enum RunCode {
        CODE_SUCCESS,
        CODE_CONFIG,
        CODE_STREAM,
        CODE_BUILDING,
        CODE_EXECUTION
    }

    private String inputFileName;
    private String outputFileName;
    private String inputConfigFileName;
    private String outputConfigFileName;
    private String substitutorConfigFileName;

    public RetCode.ConfigCode setConfig(String s) {
        Config cfg = Config.fromFile(s, GlobalConstants.CONFIG_DELIMITER);
        if (cfg == null) {
            return RetCode.ConfigCode.CODE_FAILED_TO_READ;
        }

        String inputFileName = cfg.getParameter(GlobalConstants.INPUT_FILE_FIELD);
        String outputFileName = cfg.getParameter(GlobalConstants.OUTPUT_FILE_FIELD);
        String inputConfigFileName = cfg.getParameter(GlobalConstants.INPUT_CONFIG_FILE_FIELD);
        String outputConfigFileName = cfg.getParameter(GlobalConstants.OUTPUT_CONFIG_FILE_FIELD);
        String substitutorConfigFileName = cfg.getParameter(GlobalConstants.SUBSTITUTOR_CONFIG_FILE_FIELD);

        if (inputFileName == null || outputFileName == null ||
                inputConfigFileName == null || outputConfigFileName == null) {
            return RetCode.ConfigCode.CODE_MISSING_PARAMETER;
        }

        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
        this.inputConfigFileName = inputConfigFileName;
        this.outputConfigFileName = outputConfigFileName;
        this.substitutorConfigFileName = substitutorConfigFileName;

        return RetCode.ConfigCode.CODE_SUCCESS;
    }

    public RunCode run() {

        FileReader reader = new FileReader();
        RetCode.ConfigCode configRetCode = reader.setConfig(inputConfigFileName);
        if (configRetCode != RetCode.ConfigCode.CODE_SUCCESS) {
            return RunCode.CODE_CONFIG;
        }

        FileWriter writer = new FileWriter();
        configRetCode = writer.setConfig(outputConfigFileName);
        if (configRetCode != RetCode.ConfigCode.CODE_SUCCESS) {
            return RunCode.CODE_CONFIG;
        }

        Substitutor substitutor = new Substitutor();
        configRetCode = substitutor.setConfig(substitutorConfigFileName);
        if (configRetCode != RetCode.ConfigCode.CODE_SUCCESS) {
            return RunCode.CODE_CONFIG;
        }

        FileInputStream inputStream;
        FileOutputStream outputStream;
        try {
            inputStream = new FileInputStream(inputFileName);
        }
        catch (FileNotFoundException ex) {
            return RunCode.CODE_STREAM;
        }
        try {
            outputStream = new FileOutputStream(outputFileName);
        }
        catch (FileNotFoundException ex) {
            try {
                inputStream.close();
            } catch(IOException ex1) {}
            return RunCode.CODE_STREAM;
        }

        /*
        Убрать ?
         */
        RetCode.SetterCode setterRetCode = reader.setConsumer(substitutor);
        if (setterRetCode != RetCode.SetterCode.CODE_SUCCESS) {
            return RunCode.CODE_BUILDING;
        }
        setterRetCode = reader.setInputStream(inputStream);
        if (setterRetCode != RetCode.SetterCode.CODE_SUCCESS) {
            return RunCode.CODE_BUILDING;
        }

        setterRetCode = substitutor.setProducer(reader);
        if (setterRetCode != RetCode.SetterCode.CODE_SUCCESS) {
            return RunCode.CODE_BUILDING;
        }
        setterRetCode = substitutor.setConsumer(writer);
        if (setterRetCode != RetCode.SetterCode.CODE_SUCCESS) {
            return RunCode.CODE_BUILDING;
        }

        setterRetCode = writer.setProducer(substitutor);
        if (setterRetCode != RetCode.SetterCode.CODE_SUCCESS) {
            return RunCode.CODE_BUILDING;
        }
        setterRetCode = writer.setOutputStream(outputStream);
        if (setterRetCode != RetCode.SetterCode.CODE_SUCCESS) {
            return RunCode.CODE_BUILDING;
        }

        RetCode.AlgorithmCode algRetCode = reader.execute(null);

        try {
            inputStream.close();
        }
        catch (IOException ex) {}

        try {
            outputStream.close();
        }
        catch (IOException ex) {}

        if (algRetCode != RetCode.AlgorithmCode.CODE_SUCCESS) {
            return RunCode.CODE_EXECUTION;
        }
        else {
            return RunCode.CODE_SUCCESS;
        }
    }
}