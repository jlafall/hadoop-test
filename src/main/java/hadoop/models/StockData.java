package hadoop.models;

import java.math.BigDecimal;
import org.joda.time.*;

public class StockData {
	public String date;
	public BigDecimal open;
	public BigDecimal close;
	public BigDecimal adjClose;
	public int totalCount;

	public String getMonthAndYearKey() {
		return new DateTime(date).toString("yyyy-MM");
	}

	public DateTime getDateAsJodaTime() {
		return new DateTime(date);
	}

	public void setDateFromJodaTime(DateTime joda) {
		date = joda.toString("yyyy-MM-dd");
	}
}