package ncdc;

/**
 * (unvollst�ndiger) Parser f�r NCDC Daten, vgl. Dokumentaion: FEDERAL CLIMATE
 * COMPLEX DATA DOCUMENTATION FOR INTEGRATED SURFACE DATA
 */

public class RecordParser {

	private static final int MISSING = 9999;
	private boolean valid;
	private String station, wban, airtempq;
	private int statid, year, month, day, hour, min, lat, lng, elev, airtemp;
	
	public void parse(String line) {
		valid = line.length()>92;
		if (valid) {
			station = line.substring(4, 4 + 6);
			wban = line.substring(10, 10 + 5);
			statid = (station.equals("999999")) ? -Integer.parseInt(wban)
					: Integer.parseInt(station);

			year = Integer.parseInt(line.substring(15, 15 + 4));
			month = Integer.parseInt(line.substring(19, 19 + 2));
			day = Integer.parseInt(line.substring(21, 21 + 2));
			
			hour = Integer.parseInt(line.substring(23, 23 + 2));
			min = Integer.parseInt(line.substring(25, 25 + 2));

			lat = parseWithPrefix(line.substring(28, 28 + 6));
			lng = parseWithPrefix(line.substring(34, 34 + 6));

			elev = parseWithPrefix(line.substring(46, 46 + 5));

			airtemp = parseWithPrefix(line.substring(87, 92));
			airtempq = line.substring(92, 93);
		}
	}
	
	public int parseWithPrefix(String number) {
		if (number.startsWith("+")) { 
			return Integer.parseInt(number.substring(1));
		} else {
			return Integer.parseInt(number);
		}
	}
	
	public boolean isValid() { return valid; }
	public int getStation() { return statid; }
	public int getYear() { return year; }
	public int getMonth() { return month; }
	public int getDay() { return day; }
	public int getHour() { return hour; }
	public int getMin() { return min; }
	public int getLat() { return lat; }
	public int getLong() { return lng; }
	public int getElevation() { return elev; }
	public int getAirtemp() { return airtemp; }
	public boolean isAirTempQualified() { return airtemp!=MISSING && airtempq.matches("[01459]"); }
	
}
