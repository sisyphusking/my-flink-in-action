package com.flink.demo.project.datastream.main;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class StreamWatermark {


    public static void main(String[] args)  throws Exception{

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用event time, 这里一定要设置，否则后面使用的window不执行
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> text = env.socketTextStream("localhost", 9000, "\n");
        DataStream<Tuple2<String, Long>> map = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //抽取出timestamp和生成watermark
        DataStream<Tuple2<String, Long>> watermarks = map.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long currentMaxTimeStamp = 0L;
            //定义最大延迟时间
            final Long  maxOutOfOrderness = 10000L;

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

            //生成水印的逻辑，默认100ms调用一次
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimeStamp - maxOutOfOrderness);
            }

            //定义如何提取timestamp
            @Override
            public long extractTimestamp(Tuple2<String, Long> elment, long l) {
                //获取输入数据中的第二个
                Long timestamp  = elment.f1;
                //根据当前传递进来的数据，取最大值
                this.currentMaxTimeStamp = Math.max(timestamp, this.currentMaxTimeStamp );
                Long id = Thread.currentThread().getId();

                System.out.println("current thread id: "+ id+ ", key: "+elment.f0 +", eventTime:[" + elment.f1 + " |" + sdf.format(elment.f1) + "], currentMaxTimeStamp: [" + currentMaxTimeStamp
                   + "|" + sdf.format(currentMaxTimeStamp) + "], watermark: ["+ getCurrentWatermark().getTimestamp() + "|" +sdf.format(getCurrentWatermark().getTimestamp())+ "]");

                return timestamp;
            }
        });

        DataStream<String> window = watermarks.keyBy(0)

                .window(TumblingEventTimeWindows.of(Time.seconds(3))) //按照消息的event time分配窗口，和调用TimeWindow效果一样
                // windowFunction参数：第一个输入类型，第二个输出类型，第三个key
                // allowedLateness 还可以允许数据延迟
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    // apply：数据全量的方式，取窗口中全部数据
                    // 对windows内的数据进行排序，保证数据的顺序
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        //这里的tuple就是key
                        String key = tuple.toString();
                        List<Long> arrList = new ArrayList<>();
                        //将传递进来的的tuple中的时间戳提取出来，放入到list中
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrList.add(next.f1);
                        }
                        // 将list进行排序
                        Collections.sort(arrList);

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

                        String result = key + "," + arrList.size() + "," + sdf.format(arrList.get(0)) + "," + sdf.format(arrList.get(arrList.size() - 1))
                                + "," + sdf.format(timeWindow.getStart()) + "," + sdf.format(timeWindow.getEnd());
                        out.collect(result);

                    }
                });

        window.print();

        env.execute("watermark");

        //输入的数据
        //0001,1538359882000
        //0001,1538359883000
        //0001,1538359892000
        //0001,1538359893000
        //0001,1538359894000

        //打印的结果
        //current thread id: 52, key: 0001, eventTime:[1538359882000 |2018-10-01 10:11:22,000], currentMaxTimeStamp: [1538359882000|2018-10-01 10:11:22,000], watermark: [1538359872000|2018-10-01 10:11:12,000]
        //current thread id: 52, key: 0001, eventTime:[1538359883000 |2018-10-01 10:11:23,000], currentMaxTimeStamp: [1538359883000|2018-10-01 10:11:23,000], watermark: [1538359873000|2018-10-01 10:11:13,000]
        //current thread id: 52, key: 0001, eventTime:[1538359892000 |2018-10-01 10:11:32,000], currentMaxTimeStamp: [1538359892000|2018-10-01 10:11:32,000], watermark: [1538359882000|2018-10-01 10:11:22,000]
        //current thread id: 52, key: 0001, eventTime:[1538359893000 |2018-10-01 10:11:33,000], currentMaxTimeStamp: [1538359893000|2018-10-01 10:11:33,000], watermark: [1538359883000|2018-10-01 10:11:23,000]
        //current thread id: 52, key: 0001, eventTime:[1538359894000 |2018-10-01 10:11:34,000], currentMaxTimeStamp: [1538359894000|2018-10-01 10:11:34,000], watermark: [1538359884000|2018-10-01 10:11:24,000]
        //(0001),2,2018-10-01 10:11:22,000,2018-10-01 10:11:23,000,2018-10-01 10:11:21,000,2018-10-01 10:11:24,000

        //窗口时间是2018-10-01 10:11:21,000,2018-10-01 10:11:24,000
        //上面将22秒的数据输出了，它正好落在21~24这个窗口
        //window的切分时间窗口是设定的，和数据无关，默认从00：00：00开始， 区间是左开右闭

        //窗口触发条件需同时满足：1、watermark要大于event time所属窗口的end_time （因为时间区间是划分好的，有的时候这个watermark还没到窗口的end time）
        //           2 、在[window_start_time, window_end_time)区间存在数据，注意是左闭右开

        //多次触发条件： watermark < window_end_time + allowedLateness时间内，这个窗口有late数据到达

        //多并行度情况下，会有多个线程，这个时候会取最小的watermark

        //不是对event time 要求特别严格的数据，尽量不要采用event time方式来处理数据，会有丢数据的风险






        }
}