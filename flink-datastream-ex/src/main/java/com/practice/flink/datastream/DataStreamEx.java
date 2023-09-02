package com.practice.flink.datastream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class DataStreamEx {

	public static void main(String[] args) throws Exception {
		//final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		DataStream<String> text = env.readTextFile("/Users/gchennup/Flink/flink-datastream-ex/src/main/resources/testFlink.txt");
//		
//		DataStream<Integer> dataStream = text.map(new MapFunction<String, Integer>() {
//		    @Override
//		    public Integer map(String value) {
//		        return Integer.parseInt(value);
//		    }
//		});
		
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

	

}
