import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @ProjectName: Time_On_Internet
 * @Package: mapreduce
 * @ClassName: UseInternetTimeMapper
 * @Author: 82042
 * @Description: phone use time mapper program
 * @Date: 2021/4/25 11:18
 * @Version: 1.0
 */
public class UseInternetTimeMapper extends TableMapper<Text,DoubleWritable> {
    private Text idx = new Text("sameId");
    private DoubleWritable useTime = new DoubleWritable();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        System.out.println("*************\n");
        System.out.println("mapper执行了！！！！！！！！！！");
        System.out.println("*************\n");
        byte[]  bIdx = null;
        byte[] Usetime = null;
        for(Cell cell:value.rawCells()){
            bIdx = CellUtil.cloneRow(cell);
            if("usePhone".equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                Usetime = CellUtil.cloneValue(cell);
                useTime.set(Double.parseDouble(Bytes.toString(Usetime)));
                context.write(idx, useTime);
            }
            break;
        }
        System.out.println("idx:useTime->"+Bytes.toString(bIdx)+" "+Bytes.toString(Usetime));
    }
}
