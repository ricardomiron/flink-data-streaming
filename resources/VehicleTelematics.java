package sisdistr;

import sisdistr.events.TrafficEvent;
import sisdistr.events.SpeedRadarEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple8;

public class VehicleTelematics {

    public static void main(String[] args){

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        String outFilePath = args[1];
        DataStreamSource<String> source = env.readTextFile(inFilePath);


        SingleOutputStreamOperator<TrafficEvent> filterOut = source.map(new MapFunction<String, TrafficEvent>() {
			TrafficEvent out = new TrafficEvent();

                @Override
            public TrafficEvent map(String in) throws Exception {
                String[] fieldArray = in.split(",");
				out.setTime(Integer.parseInt(fieldArray[0]));
				out.setVid(Integer.parseInt(fieldArray[1]));
				out.setSpeed(Integer.parseInt(fieldArray[2]));
				out.setHighway(Integer.parseInt(fieldArray[3]));
				out.setLane(Integer.parseInt(fieldArray[4]));
				out.setDirection(Integer.parseInt(fieldArray[5]));
				out.setSegment(Integer.parseInt(fieldArray[6]));
				out.setPosition(Integer.parseInt(fieldArray[7]));

                return out;
            }
        });

        SingleOutputStreamOperator speedFines = SpeedRadar.something(filterOut);

		 speedFines.writeAsCsv(outFilePath);

        try {
            env.execute("SpeedRadar");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
