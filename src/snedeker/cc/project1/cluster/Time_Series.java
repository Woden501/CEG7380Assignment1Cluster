package snedeker.cc.project1.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


//provide the time series pair (timestamp, value)
public class Time_Series 
   implements Writable, Comparable<Time_Series> {

	private long timestamp;
	private double value;
	
	public static Time_Series copy(Time_Series tsd) {
		return new Time_Series(tsd.timestamp, tsd.value);
	}
	
	public Time_Series(long timestamp, double value) {
		set(timestamp, value);
	}
	
	public Time_Series() {
	}
	
	public void set(long timestamp, double value) {
		this.timestamp = timestamp;
		this.value = value;
	}	
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public double getValue() {
		return this.value;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.timestamp  = in.readLong();
		this.value  = in.readDouble();
	}

	//convert a binary into time series data
	public static Time_Series read(DataInput in) throws IOException {
		Time_Series tsData = new Time_Series();
		tsData.readFields(in);
		return tsData;
	}

	public String getDate() {
		return Date_Util.getStringDate(this.timestamp); //need to import Date_Util class	
	}

   //clone this object 
    public Time_Series clone() {
       return new Time_Series(timestamp, value);
    }

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.timestamp );
		out.writeDouble(this.value );

	}

	//sort the time series data in the reducer
	@Override
	public int compareTo(Time_Series data) {
		if (this.timestamp  < data.timestamp ) {
			return -1;
		} 
		else if (this.timestamp  > data.timestamp ) {
			return 1;
		}
		else {
		   return 0;
		}
	}
	
	public String toString() {
       return "("+timestamp+","+value+")";
    }
}
