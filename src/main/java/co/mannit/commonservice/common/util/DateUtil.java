package co.mannit.commonservice.common.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

	
	public static String getCurrentDateTime() {
//		SimpleDateFormat df = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss z");
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return df.format(new Date());
	}
	
	public static void main(String[] args) {
		System.out.println(getCurrentDateTime());
	}
}
