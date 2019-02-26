import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: xu.dm
 * @Date: 2019/2/25 21:45
 * @Description:
 */
public class AvroSort extends Configured implements Tool {
    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     * @return exit code.
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.job.ubertask.enable","true");

        Job job = Job.getInstance(conf,"Avro sort");
        job.setJarByClass(AvroSort.class);

        //通过AvroJob直接设置Avro key和value的输入和输出，而不是使用Job来设置
        AvroJob.setMapOutputKeySchema(job, AvroSchemas.SCHEMA);
        AvroJob.setMapOutputValueSchema(job,AvroSchemas.SCHEMA);
//        AvroJob.setOutputKeySchema(job,AvroSchemas.SCHEMA);

        job.setMapperClass(AvroMapper.class);
        job.setReducerClass(AvroReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        //也可以输出文本格式，AvroKey会被转换成json文本模式
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        Path outPath = new Path(args[1]);
        FileSystem fileSystem = outPath.getFileSystem(conf);
        //删除输出路径
        if(fileSystem.exists(outPath))
        {
            fileSystem.delete(outPath,true);
        }

        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new AvroSort(),args);
        System.exit(exitCode);
    }
}
