
package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AvgSpeedControl {

    public static SingleOutputStreamOperator detectAvg (SingleOutputStreamOperator<TrafficEvent> tuples){
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
                        return trafficEvent.getTime() * 1000;
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
                .apply(new AvgWindowFunction());

    }

    private static class AvgWindowFunction implements WindowFunction<TrafficEvent, AvgEvent,
            Tuple3<Integer, Integer, Integer>, TimeWindow> {

        private AvgEvent avgEvent = new AvgEvent();

        @Override
        public void apply(Tuple3<Integer, Integer, Integer> key, TimeWindow window,
                          Iterable<TrafficEvent> in, Collector<AvgEvent> collector) throws Exception {

            int initialTime = Integer.MAX_VALUE;
            int initialPos = Integer.MAX_VALUE;
            int finalTime = Integer.MIN_VALUE;
            int finalPos = Integer.MIN_VALUE;
            int totalEvents = Iterables.size(in);

            if (totalEvents < 5) {
                return;
            }

            for (TrafficEvent event : in) {
                initialTime = Integer.min(initialTime, event.getTime());
                initialPos = Integer.min(initialPos, event.getPosition());
                finalTime = Integer.max(finalTime, event.getTime());
                finalPos = Integer.max(finalPos, event.getPosition());
            }

            double average = (finalPos - initialPos) * 1.0 / (finalTime - initialTime) * 2.23694;

            if (average > 60) {
                AvgEvent.setEntryTime(initialTime);
                AvgEvent.setExitTime(finalTime);
                AvgEvent.setVid(key.f0);
                AvgEvent.setHighway(key.f1);
                AvgEvent.setDirection(key.f2);
                AvgEvent.setAvg(average);
                collector.collect(avgEvent);
            }


        }
    }

}
