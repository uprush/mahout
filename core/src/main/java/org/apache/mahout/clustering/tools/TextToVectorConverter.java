package org.apache.mahout.clustering.tools;

import java.io.IOException;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;

/**
 * This class converts a set of input data in the text file format to vectors in
 * the sequence file format. The Text file input should have a {@link Text} key
 * containing a unique identifier and {@link Text} values
 * 
 * @author hamadakoichi
 */
public class TextToVectorConverter extends AbstractJob {

	/**
	 * Convert input data in the {@link Text} format to vectors in the
	 * {@link SequenceFile} format.
	 * 
	 * @param input
	 *            input directory of the {@link Text} format
	 * @param output
	 *            output directory where
	 *            {@link org.apache.mahout.math.DenseVector}s in the
	 *            {@link SequenceFile} format are generated
	 */
	public static void convert(Configuration conf, Path input, Path output)
			throws IOException, InterruptedException, ClassNotFoundException {

		Job job = new Job(conf);
		job.setJobName("TextToDenseVectorConverter");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(DenseVectorizeMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setJarByClass(TextToVectorConverter.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (job.waitForCompletion(true) == false) {
			throw new InterruptedException(
					"TextToDenseVectorConverter Job failed processing"
							+ input.toString());
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TextToVectorConverter(), args);
	}

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException, InstantiationException, IllegalAccessException {

		addInputOption();
		addOutputOption();
		if (parseArguments(args) == null) {
			return -1;
		}

		Path input = getInputPath();
		Path output = getOutputPath();
		run(getConf(), input, output);
		return 0;
	}

	/**
	 * Run the job using supplied arguments
	 * 
	 * @param conf
	 * @param textInputDir
	 *            input directory of the {@link Text} format
	 * @param seqOutputDir
	 *            output directory where
	 *            {@link org.apache.mahout.math.DenseVector}s in the
	 *            {@link SequenceFile} format are generated
	 **/
	public static void run(Configuration conf, Path textInputDir,
			Path seqOutputDir) throws IOException, InterruptedException,
			ClassNotFoundException, InstantiationException, IllegalAccessException {
		TextToVectorConverter.convert(conf, textInputDir, seqOutputDir);
	}
}