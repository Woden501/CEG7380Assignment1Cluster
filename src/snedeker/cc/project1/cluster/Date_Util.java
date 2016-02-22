package snedeker.cc.project1.cluster;

import java.text.SimpleDateFormat;
import java.util.Date;


//provide a basic Date utility functions
public class Date_Util {

	static final String DATE_FORMAT = "yyyy-MM-dd";
	static final SimpleDateFormat SIMPLE_DATE_FORMAT = 
	   new SimpleDateFormat(DATE_FORMAT);

    //return Date format through string date
	public static Date getDate(String str_date)  {
        try {
        	return SIMPLE_DATE_FORMAT.parse(str_date);
        }
        catch(Exception e) {
        	return null;
        }
	}
	
	public static String getStringDate(long timestamp) {
        return SIMPLE_DATE_FORMAT.format(timestamp);
	}	
	
}
