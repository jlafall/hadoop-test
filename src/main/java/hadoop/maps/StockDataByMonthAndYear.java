package hadoop.maps;

import org.apache.crunch.MapFn;
import hadoop.models.StockData;

public class StockDataByMonthAndYear extends MapFn<StockData, String> {
	
	@Override
	public String map(StockData data) {
		return data.getMonthAndYearKey();
	}
}