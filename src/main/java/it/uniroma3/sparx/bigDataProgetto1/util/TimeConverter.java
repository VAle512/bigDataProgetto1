package it.uniroma3.sparx.bigDataProgetto1.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeConverter {
	
	public static String unix2String(long time) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(time*1000);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
		return sdf.format(calendar.getTime()) ;
	}

}
