import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @ProjectName: Time_On_Internet_MapReduce
 * @Package: mapreduce
 * @ClassName: HBaseMR2HBase
 * @Author: 82042
 * @Description: driver
 * @Date: 2021/4/25 16:21
 * @Version: 1.0
 */
public class HBaseMR2HBase {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("启动了！！！！");
        Configuration conf = new Configuration();
        System.out.println(conf);
        String[] ioArgs = new String[]{"stu_info", "useTime"};
        if(args.length == 2){
            ioArgs[0] = args[0];
            ioArgs[1] = args[1];
        }else if(args.length == 1 || args.length > 2){
            System.err.println("Usage: edu.wenhua.hbase.mr.HBaseMR2HBase [<tablename_in> <tablename_out>]");
            System.exit(2);
        }
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        Job job = Job.getInstance(conf, "HBaseMR2HBase");
        job.setJarByClass(HBaseMR2HBase.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("time"), Bytes.toBytes("usePhone"));
        //默认值为1，但对MapReduce作业不利
        scan.setCaching(500);
        //对MapReduce作业不要设为true
        scan.setCacheBlocks(false);

        //通过TableMapReduceUtil.initTableMapperJob设置Mapper输入输出的参数，并设置输出的键/值、输出类、输出目录等。
        //输入的表名
        TableMapReduceUtil.initTableMapperJob(otherArgs[0],
                //Scan实例，控制列族及属性的选择
                scan,
               //TableMapper类
               UseInternetTimeMapper.class,
                //mapper输出键的类型
                Text.class,
                //mapper输出值的类型
                DoubleWritable.class,
                job);

        //可以考虑在此处加入：输出表的创建......

        //设置输出的数据表：请先使用hbase shell建立此表：create 'score','average'
        TableMapReduceUtil.initTableReducerJob(otherArgs[1],
                //设置TableReducer类
                UseInternetTimeReducer.class,
                job);

        job.setNumReduceTasks(1);

        boolean b = job.waitForCompletion(true);
        if(!b){
            throw new IOException("error with job!");
        }
    }

}
