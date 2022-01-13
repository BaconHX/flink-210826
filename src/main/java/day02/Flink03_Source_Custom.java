package day02;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink03_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.TODO 自定义source  自定义source想实现多并行度,必须实现ParallelSourceFunction接口
        DataStreamSource<WaterSensor> streamSource = env.addSource(new mySource()).setParallelism(2);

        streamSource.print();

        env.execute();

    }

    //自定义一个类实现sourceFunction  自定义source想实现多并行度,必须实现ParallelSourceFunction接口
//    public static class mySource implements SourceFunction<WaterSensor>{
    public static class mySource implements ParallelSourceFunction<WaterSensor> {

        private Boolean isRunning = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning){
                ctx.collect(new WaterSensor("sensor_"+random.nextInt(1000),System.currentTimeMillis(),random.nextInt(2000)));
                Thread.sleep(200);
            }

        }

        @Override
        public void cancel() {
            isRunning=false;

        }
    }

}
