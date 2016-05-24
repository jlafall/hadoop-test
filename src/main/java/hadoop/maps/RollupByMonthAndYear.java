package hadoop.maps;

import org.apache.crunch.MapFn;
import hadoop.models.StockData;
import org.joda.time.*;
import com.google.common.collect.Iterables;

public class RollupByMonthAndYear extends MapFn<Iterable<StockData>, StockData> {
	
	@Override
	public StockData map(Iterable<StockData> datas) {
		StockData[] dataArray = Iterables.toArray(datas, StockData.class);
		DateTime baseDate = dataArray[0].date;

		double totalOpen = 0.0;
		double totalClose = 0.0;
		double totalAdjClose = 0.0;

		for (StockData d : dataArray) {
			totalOpen += d.open;
			totalClose += d.close;
			totalAdjClose += d.adjClose;
		}

		StockData data = new StockData();
		data.date = baseDate.plusDays(-1 * baseDate.getDayOfWeek());
		data.open = totalOpen;
		data.close = totalClose;
		data.adjClose = totalAdjClose;

		return data;
	}
}