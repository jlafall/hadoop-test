package hadoop.jobs;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.io.To;
import org.apache.crunch.io.From;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.impl.mr.MRPipeline;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.models.StockData;
import hadoop.maps.StockDataByMonthAndYear;
import hadoop.combiners.RollupByMonthAndYear;
import hadoop.operations.CsvToStockData;
import hadoop.operations.StockDataToCsv;

public class TestCrunch01 extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		// set configurations for the job
		Configuration conf = getConf();
		conf.setBoolean("crunch.log.job.progress", true);

		// create PTypes for POJOs
		PType<StockData> pStockDataType = Avros.reflects(StockData.class);

		// create the pipeline
		Pipeline pipeline = new MRPipeline(TestCrunch01.class, conf);

		// get data from input text file
		PCollection<String> lines = pipeline.read(From.textFile(inputPath));

		// convert CSV row into StockData objects
		PCollection<StockData> stockRows = lines.parallelDo(new CsvToStockData(), pStockDataType);

		// create key by month and year
		PTable<String, StockData> keyByMonthAndYear = stockRows.by(new StockDataByMonthAndYear(), Avros.strings());

		// rollup data by month and year
		PTable<String, StockData> rollupByMonthAndYear = keyByMonthAndYear.groupByKey()
			// .mapValues(new RollupByMonthAndYear(), pStockDataType);
			.combineValues(new RollupByMonthAndYear());

		// sort table by key in descending order
		PTable<String, StockData> sortedRollup = Sort.sort(rollupByMonthAndYear, Sort.Order.DESCENDING);

		// change rollup StockData object back to CSV format
		PCollection<String> csvFormat = sortedRollup.values()
			.parallelDo(new StockDataToCsv(), Avros.strings());

		// output results to text file
		csvFormat.write(To.textFile(outputPath));

		// get results
		PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new TestCrunch01(), args);
		System.exit(result);
	}
}