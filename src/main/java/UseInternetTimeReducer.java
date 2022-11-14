import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @ProjectName: Time_On_Internet
 * @Package: mapreduce
 * @ClassName: UseInternetTimeReducer
 * @Author: 82042
 * @Description:
 * @Date: 2021/4/25 14:18
 * @Version: 1.0
 */

public class UseInternetTimeReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {
    /**
     * 逻辑
     * k2   v2
     * 0001 2
     * 0002 3
     * 0003 4
     * 0005 6
     * 0007 7
     * k3   v3
     * 0-2  0
     * 2-5  3
     * 5-8  2
     * 8--  0
     */
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,
                          Context context)
            throws IOException, InterruptedException{
        System.out.println("*************\n");
        System.out.println("reducer执行了！！！！！！！！！！");
        System.out.println("*************\n");
        int []t={0,0,0,0,0,0,0,0};
        for (DoubleWritable value : values) {
            //0-2
            if(0<=value.get()&&value.get()<2){
                t[1]++;
            }
            //2-5
            else if (2<=value.get()&&value.get()<5){
                t[2]++;
            }
            //5-8
            else if (5<=value.get()&&value.get()<8){
                t[3]++;
            }
            //8--
            else {
                t[4]++;
            }
        }
        key=new Text("0-2");
        //行键
        Put put1 = new Put(Bytes.toBytes(key.toString()));
        put1.addColumn(Bytes.toBytes("time"),Bytes.toBytes("internetTime"),Bytes.toBytes(String.valueOf(t[1])));
        context.write(null,put1);
        key=new Text("2-5");
        Put put2 = new Put(Bytes.toBytes(key.toString()));
        put2.addColumn(Bytes.toBytes("time"),Bytes.toBytes("internetTime"),Bytes.toBytes(String.valueOf(t[2])));
        context.write(null,put2);
        key=new Text("5-8");
        Put put3 = new Put(Bytes.toBytes(key.toString()));
        put3.addColumn(Bytes.toBytes("time"),Bytes.toBytes("internetTime"),Bytes.toBytes(String.valueOf(t[3])));
        context.write(null,put3);
        key=new Text("8--");
        Put put4 = new Put(Bytes.toBytes(key.toString()));
        put4.addColumn(Bytes.toBytes("time"),Bytes.toBytes("internetTime"),Bytes.toBytes(String.valueOf(t[4])));
        context.write(null,put4);
    }
}
