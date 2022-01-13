package day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {



        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从集合中获取数据(不能设置多并行度)
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
//        DataStreamSource<Integer> streamSource = env.fromCollection(list);

        //从文件读取数据,也可以读取hdfs文件,要先导入相关依赖,要注意hadoop是否高可用,高科应时,应该写这个路径hdfs://mycluster/......
//        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:8020/hello.txt").setParallelism(2);


        //TODO 从端口读取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);


        //TODO 从元素中读取数据(做测试的时候用)
        DataStreamSource<String> streamSource = env.fromElements("a", "b", "c", "d");

        streamSource.print();

        env.execute();
    }
}
