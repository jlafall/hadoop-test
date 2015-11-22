package hadoop.operations;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

import java.math.BigDecimal;
import org.joda.time.*;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import hadoop.models.StockData;

public class CsvToStockData extends DoFn<String, StockData> {
	private static final Splitter spliter = Splitter.onPattern(",");

	@Override
	public void process(String line, Emitter<StockData> emitter) {
		StockData data = new StockData();

		// get columns from row
		String[] columns = Iterables.toArray(spliter.split(line), String.class);

		// get column data and put into StockData object
		data.setDateFromJodaTime(new DateTime(columns[0].trim()));
		data.open = new BigDecimal(columns[1].trim());
		data.close = new BigDecimal(columns[4].trim());
		data.adjClose = new BigDecimal(columns[6].trim());

		// emit stock data object, one per row
		emitter.emit(data);
	}
}