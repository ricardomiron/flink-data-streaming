package sisdistr;

import sisdistr.events.TrafficEvent;
import sisdistr.events.AvgEvent;

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
        .apply(); //To Do...


        .map(new MapFunction<TrafficEvent, AvgEvent>() {

          AvgEvent out = new AvgEvent();

          @Override
          public AvgEvent map(TrafficEvent in) throws Exception {
            out.setTime(in.getTime());
            out.setVid(in.getVid());
            out.setHighway(in.getHighway());
            out.setSegment(in.getSegment());
            out.setDirection(in.getDirection());
            out.setSpeed(in.getSpeed());

            return out;
          }
        });
    }
}
