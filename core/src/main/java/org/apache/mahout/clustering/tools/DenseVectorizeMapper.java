package org.apache.mahout.clustering.tools;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * ConvertMapper for DenseVector
 * 
 * @author hamadakoichi
 */
public class DenseVectorizeMapper extends
		Mapper<LongWritable, Text, Text, VectorWritable> {

	private Text id = new Text();
	private VectorWritable point = new VectorWritable();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] rowdata = line.split(",", -1);

		String sid = rowdata[0];
		id.set(sid);

		double[] dpoint = new double[rowdata.length - 1];
		for (int i = 1; i < rowdata.length; i++) {
			dpoint[i - 1] = Double.valueOf(rowdata[i]);
		}
		Vector vpoint = new DenseVector(dpoint);
		NamedVector nvec = new NamedVector(vpoint, sid);
		point.set(nvec);

		context.write(id, point);
	}
}
