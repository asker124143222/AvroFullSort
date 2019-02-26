import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/2/25 17:17
 * @Description:
 */
public class AvroReducer extends Reducer<AvroKey<GenericRecord>,AvroValue<GenericRecord>,IntPair,NullWritable> {
    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        //在混洗阶段完成排序，reducer只需直接输出数据
        for (AvroValue<GenericRecord> value : values){
            GenericRecord record = value.datum();
            context.write(new IntPair((Integer) record.get("year"),(Integer)(record.get("temperature"))),NullWritable.get());
        }
    }

    private GenericRecord newRecord(GenericRecord value){
        GenericRecord record = new GenericData.Record(AvroSchemas.SCHEMA);
        record.put("year",value.get("year"));
        record.put("temperature",value.get("temperature"));

        return record;
    }
}
