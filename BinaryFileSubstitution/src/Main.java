import ru.spbstu.pipeline.RetCode;

public class Main {

    /*
    1. Интерфейс вообще
        дублирование setConsumer, setProducer
    2. RetCode
        один или несколько
        отдельный для SubstitutionTable
    3. Какие параметры в конфигах
    4. PipelineManager->run куча if'ов
     */

    public static void main(String[] Args)  {
//        if (Args == null || Args.length != 1) {
//            System.out.println("Expected one command-line argument.");
//            return;
//        }
//        String configFileName = Args[0];
        String configFileName = "./config.txt";

        PipelineManager manager = new PipelineManager();
        RetCode.ConfigCode configRetCode = manager.setConfig(configFileName);

        if (configRetCode != RetCode.ConfigCode.CODE_SUCCESS) {
            System.out.println("Failed to read config");
            return;
        }

        PipelineManager.RunCode retCode = manager.run();

        if (retCode != PipelineManager.RunCode.CODE_SUCCESS) {
            System.out.println("ERROR " + retCode);
        }
        else {
            System.out.println("DONE");
        }
    }
}
