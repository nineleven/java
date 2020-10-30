import ru.spbstu.pipeline.RC;

import java.util.logging.Logger;

public class Main {

    /*
    1. Logger в конструкторе Executable
    2. GlobalConstants?
    3. PipelineManger -> SemanticConfigValidator new ...(..., logger) норм??
     */

    public static void main(String[] Args)  {
        if (Args == null || Args.length != 1) {
            System.out.println("Expected one command-line argument");
            return;
        }
        String configFileName = Args[0];

        Logger logger = Logger.getLogger("Logger");

        PipelineManager manager = PipelineManager.createInstance(configFileName, logger);
        if (manager == null) {
            logger.severe("Failed to create a pipeline manager");
            return;
        }

        RC retCode = manager.run();

        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Pipeline execution failed");
            return;
        }

        System.out.println("DONE");
    }
}
