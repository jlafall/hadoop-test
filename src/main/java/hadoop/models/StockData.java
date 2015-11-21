package hadoop.models;
import org.joda.time.*;

public class StockData {
	public String date;
	public double open;
	public double close;
	public double adjClose;

	public String getMonthAndYearKey() {
		return new DateTime(date).toString("yyyyMM");
	}

	public DateTime getDateAsJodaTime() {
		return new DateTime(date);
	}

	public void setDateFromJodaTime(DateTime joda) {
		date = joda.toString("yyyy-MM-dd");
	}
}