import org.apache.avro.Schema;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/2/25 21:51
 * @Description:
 */
public class AvroSchemas {
    private Schema currentSchema;

    //本例中不使用常量，修改成资源中加载
    public static final Schema SCHEMA = new Schema.Parser().parse("{\n" +
            "\t\"type\":\"record\",\n" +
            "\t\"name\":\"WeatherRecord\",\n" +
            "\t\"doc\":\"A weather reading\",\n" +
            "\t\"fields\":[\n" +
            "\t\t{\"name\":\"year\",\"type\":\"int\"},\n" +
            "\t\t{\"name\":\"temperature\",\"type\":\"int\",\"order\":\"descending\"}\n" +
            "\t]\t\n" +
            "}");

    public AvroSchemas() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        //采用从资源文件中读取Avro数据格式
        this.currentSchema = parser.parse(getClass().getResourceAsStream("dataRecord.avsc"));
    }


    public Schema getCurrentSchema() {
        return currentSchema;
    }
}
