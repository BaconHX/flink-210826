package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_WordCount_Stream_Bounded {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将并行度设置为1
        env.setParallelism(1);

        //2.读取文件数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.将一行数据按照空格切分,返回一个个单词
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //将一行数据切分
                String[] words = value.split(" ");
                //遍历收集每一个单词
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.将单词转换为Tuple2元组(Lambda写法) //注意:lambda表达式会有泛型擦除,所以要手动添加泛型(.returns)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDStream.map(value -> Tuple2.of(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        //5.将相同的单词聚合到一起
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

//        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);  //返回值类型是tuple

        //6.累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //7.打印结果到控制台
        result.print();

        //8.执行程序 //流式处理必须要写执行语句
        env.execute();

    }
}
