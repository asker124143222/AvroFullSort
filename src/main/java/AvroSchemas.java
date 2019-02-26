import org.apache.avro.Schema;

/**
 * @Author: xu.dm
 * @Date: 2019/2/25 21:51
 * @Description:
 */
public class AvroSchemas {
    public static final Schema SCHEMA = new Schema.Parser().parse("{\n" +
            "\t\"type\":\"record\",\n" +
            "\t\"name\":\"WeatherRecord\",\n" +
            "\t\"doc\":\"A weather reading\",\n" +
            "\t\"fields\":[\n" +
            "\t\t{\"name\":\"year\",\"type\":\"int\"},\n" +
            "\t\t{\"name\":\"temperature\",\"type\":\"int\",\"order\":\"descending\"}\n" +
            "\t]\t\n" +
            "}");

}
