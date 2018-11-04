package sisdistr.events;
import org.apache.flink.api.java.tuple.Tuple6;

public class SpeedRadarEvent extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {

    public SpeedRadarEvent() {
    }

    public void setTime(int time) {
        f0 = time;
    }

    public void setVid(int vid) {
        f1 = vid;
    }

    public void setHighway(int highway) {
        f2 = highway;
    }

    public void setSegment(int segment) {
        f3 = segment;
    }

    public void setDirection(int direction) {
        f4 = direction;
    }

    public void setSpeed(int speed) {
        f5 = speed;
    }

}
