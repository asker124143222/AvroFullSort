import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/2/25 16:23
 * @Description:
 */
public class AvroMapper extends Mapper<LongWritable,Text,AvroKey<GenericRecord>,AvroValue<GenericRecord>> {
    private RecordParser parser = new RecordParser();
//    private GenericRecord record = new GenericData.Record(AvroSchemas.SCHEMA);
    private AvroSchemas schema;
    private GenericRecord record;

    public AvroMapper() throws IOException {
        schema =new AvroSchemas();
        record = new GenericData.Record(schema.getCurrentSchema());
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        parser.parse(value.toString());
        if(parser.isValid()){
            record.put("year",parser.getYear());
            record.put("temperature",parser.getData());
            context.write(new AvroKey<>(record),new AvroValue<>(record));
        }
    }
}
