
package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class AvgSpeedControl {

    public static SingleOutputStreamOperator detectavg (SingleOutputStreamOperator<TrafficEvent> tuples){
        return tuples

                //Filter tuples that are between segments 52 and 56
                .filter(new FilterFunction<TrafficEvent>() {
                    @Override
                    public boolean filter(TrafficEvent in) throws Exception {
                        if(in.getSegment() >= 52 && in.getSegment() <= 56){ return true;
                        }else{return false;}
                    }
                })

                //Assign Timestamps and Watermarks
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TrafficEvent>() {
                    @Override
                    public long extractAscendingTimestamp(TrafficEvent trafficEvent) {
                        return trafficEvent.getTime();
                    }
                })

                //Convert the DataStream into a KeyedStream using the specified keys
                .keyBy(new KeySelector<TrafficEvent, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> getKey(TrafficEvent trafficEvent) throws Exception {
                        return Tuple3.of(trafficEvent.getVid(), trafficEvent.getHighway(), trafficEvent.getDirection());
                    }
                })

                //Create a Session Window
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))

                //Detects cars with an average speed higher than 60 mph
//                .apply(AvgSpeedControl.speedCalculation)
//
//
//
//
//                .map(new MapFunction<TrafficEvent, AvgEvent>() {
//
//                    AvgEvent out = new AvgEvent();
//
//                    @Override
//                    public AvgEvent map(TrafficEvent in) throws Exception {
//                        out.setEntryTime(in.getTime());
//                        out.setExitTime(in.getTime());
//                        out.setVid(in.getVid());
//                        out.setHighway(in.getHighway());
//                        out.setDirection(in.getDirection());
//
//                        return out;
//                    }
//                });
    }

    private static void speedCalculation(final Tuple3<Long, Integer, Integer> key, final TimeWindow window, final Iterable<TrafficEvent> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) {
        final int appearances = Iterables.size(input);
        if (appearances < 5) {
            return;
        }

    }

}

