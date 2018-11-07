package sisdistr.events;
import org.apache.flink.api.java.tuple.Tuple6;

public class AvgEvent extends Tuple6<Integer, Integer, Integer, Integer, Integer, Double> {

    public AvgEvent() {
    }

    public void setEntryTime(int time) {
        f0 = time;
    }

    public void setExitTime(int time) {
        f1 = time;
    }

    public void setVid(String vid) {
        f2 = vid;
    }

    public void setHighway(int highway) {
        f3 = highway;
    }

    public void setDirection(int direction) {
        f4 = direction;
    }

    public void setAvg(double avg) {
        f5 = avg;
    }

}
