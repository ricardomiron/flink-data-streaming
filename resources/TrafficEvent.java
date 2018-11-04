package sisdistr.events;

import org.apache.flink.api.java.tuple.Tuple8;

public class TrafficEvent extends Tuple8<Integer, Integer, Integer,
        Integer, Integer, Integer, Integer, Integer> {

    public TrafficEvent() {
    }

    public int getTime() {
        return f0;
    }

    public Integer getVid() {
        return f1;
    }

    public Integer getSpeed() {
        return f2;
    }

    public Integer getHighway() {
        return f3;
    }

	public Integer getLane() {
		return f4;
	}

    public Integer getDirection() {
        return f5;
    }

    public Integer getSegment() {
        return f6;
    }

	public Integer getPosition() {
		return f7;
	}

    public void setTime(int time) {
        f0 = time;
    }

    public void setVid(int vid) {
        f1 = vid;
    }

    public void setSpeed(int speed) {
        f2 = speed;
    }

    public void setHighway(int highway) {
        f3 = highway;
    }

    public void setLane(int lane) {
        f4 = lane;
    }

    public void setDirection(int direction) {
        f5 = direction;
    }

    public void setSegment(int segment) {
        f6 = segment;
    }

    public void setPosition(int position) {
        f7 = position;
    }

	String speedFineOutputFormat(TrafficEvent output) {
       return String.format("%s,%s,%s,%s,%s,%s", output.getTime(), output.getVid(), output.getHighway(), output.getSegment(), output.getDirection(), output.getSpeed());
	}

}
