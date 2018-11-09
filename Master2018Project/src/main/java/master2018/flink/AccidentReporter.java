package master2018.flink;

import sisdistr.events.AccidentEvent;
import sisdistr.events.TrafficEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AccidentReporter {

    public static SingleOutputStreamOperator reportAcc (SingleOutputStreamOperator<TrafficEvent> tuples){
      return tuples
        .filter(new FilterFunction<TrafficEvent>() {
            @Override
            public boolean filter(TrafficEvent in) throws Exception {
                if(in.getSpeed() == 0){ return true;  //filtering the vehicles which speed equals 0
                }else{return false;}
            }
        })
        .keyBy(new KeySelector<TrafficEvent, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<Integer, Integer, Integer, Integer, Integer> getKey(TrafficEvent trafficEvent) throws Exception {
                return Tuple5.of(trafficEvent.getVid(), trafficEvent.getHighway(), trafficEvent.getDirection(), trafficEvent.getSegment(), trafficEvent.getPosition());
              }
          }).countWindow(4, 1)
        .apply(new AccidentWindowFunction());
    }

    private static class AccidentWindowFunction implements WindowFunction<TrafficEvent, AccidentEvent,
            Tuple5<Integer, Integer, Integer, Integer, Integer>, GlobalWindow> {

        private TrafficEvent lastEvent = new TrafficEvent();
        private AccidentEvent accidentEvent = new AccidentEvent();

        @Override
        public void apply(Tuple5<Integer, Integer, Integer, Integer, Integer> key, GlobalWindow window,
                          Iterable<TrafficEvent> in, Collector<AccidentEvent> collector) throws Exception {

            int counter = 1;
            //Using Iterator to browse traffic events
            Iterator<TrafficEvent> iterator = in.iterator();
            int time1 = iterator.next().getTime();

            while (iterator.hasNext()) {
                counter++;
                lastEvent = iterator.next();
                //checking if the vehicle reports at least 4 consecutive events from the same position
                if (counter == 4) {
                    accidentEvent.setTime1(time1);
                    accidentEvent.setTime2(lastEvent.f0);
                    accidentEvent.setVid(key.f0);
                    accidentEvent.setHighway(key.f1);
                    accidentEvent.setSegment(key.f3);
                    accidentEvent.setDirection(key.f2);
                    accidentEvent.setPosition(key.f4);
                    collector.collect(accidentEvent);
                }
            }
        }
 }
}
