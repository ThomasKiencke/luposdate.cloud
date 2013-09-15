package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

public class TestLoadUDF extends LoadFunc implements StoreFuncInterface,
LoadPushDown, OrderedLoadFunc{

	@Override
	public WritableComparable<?> getSplitComparable(InputSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<OperatorSet> getFeatures() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
