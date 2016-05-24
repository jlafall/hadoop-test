package hadoop.models;
import org.joda.time.*;

public class StockData {
	public DateTime date;
	public double open;
	public double close;
	public double adjClose;

	public String getMonthAndYearKey() {
		return date.toString("yyyyMM");
	}
}