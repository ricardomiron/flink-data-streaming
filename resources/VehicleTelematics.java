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

    // main() defines and executes the DataStream program
    public static void main(String[] args){

        //Set up the streaming execution environment
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        String outFilePath = args[1];
        //Create a DataStream[SensorReading] from a stream source
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

        //Apply streaming transformations to implement the application logic
        SingleOutputStreamOperator speedFines = SpeedRadar.detectspeed(filterOut);

      //Output the result to a data sink
		  speedFines.writeAsCsv(outFilePath);

      // speedFines.writeAsCsv(Paths.get(outputPath, "speedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
      //           .setParallelism(1);

        //Execute application
        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
