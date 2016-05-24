package hadoop.combiners;

import hadoop.common.CombinerFn;
import hadoop.models.StockData;
import java.math.BigDecimal;
import org.joda.time.*;

public class RollupByMonthAndYear extends CombinerFn<String, StockData> {
	
	@Override
	public StockData combine(Iterable<StockData> datas) {
		DateTime baseDate = null;

		BigDecimal totalOpen = BigDecimal.ZERO;
		BigDecimal totalClose = BigDecimal.ZERO;
		BigDecimal totalAdjClose = BigDecimal.ZERO;
		int totalCount = 0;

		for (StockData d : datas) {
			if (baseDate == null) {
				baseDate = d.getDateAsJodaTime();
			}

			totalOpen = totalOpen.add(d.open);
			totalClose = totalClose.add(d.close);
			totalAdjClose = totalAdjClose.add(d.adjClose);

			++totalCount;
		}

		StockData data = new StockData();
		data.setDateFromJodaTime(baseDate.plusDays(-1 * baseDate.getDayOfWeek()));
		data.open = totalOpen;
		data.close = totalClose;
		data.adjClose = totalAdjClose;
		data.totalCount = totalCount;

		return data;
	}
}
