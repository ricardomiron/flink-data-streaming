package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class SpeedRadar {

    public static SingleOutputStreamOperator detectSpeed (SingleOutputStreamOperator<TrafficEvent> tuples){
      return tuples
        .filter(new FilterFunction<TrafficEvent>() {
            @Override
            public boolean filter(TrafficEvent in) throws Exception {
                if(in.getSpeed() >= 90){ return true;
                }else{return false;}
            }
        })
        .map(new MapFunction<TrafficEvent, SpeedRadarEvent>() {

          SpeedRadarEvent out = new SpeedRadarEvent();

          @Override
          public SpeedRadarEvent map(TrafficEvent in) throws Exception {
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
