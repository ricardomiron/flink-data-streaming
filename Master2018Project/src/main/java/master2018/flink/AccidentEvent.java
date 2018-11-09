package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple7;

public class AccidentEvent extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    public AccidentEvent() {
    }

    public void setTime1(int time) {
        f0 = time;
    }

    public void setTime2(int time) {
        f1 = time;
    }

    public void setVid(int vid) {
        f2 = vid;
    }

    public void setHighway(int highway) {
        f3 = highway;
    }

    public void setSegment(int segment) {
        f4 = segment;
    }

    public void setDirection(int direction) {
        f5 = direction;
    }

    public void setPosition(int position) {
        f6 = position;
    }

}
