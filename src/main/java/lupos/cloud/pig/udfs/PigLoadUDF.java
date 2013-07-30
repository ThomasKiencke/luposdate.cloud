package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PigLoadUDF extends LoadFunc {

	 protected RecordReader in = null;
	    private byte fieldDel = '\t';
	    private ArrayList<Object> mProtoTuple = null;
	    private TupleFactory mTupleFactory = TupleFactory.getInstance();
	    private static final int BUFFER_SIZE = 1024;

	    public PigLoadUDF() {
	    }

	    /**
	     * Constructs a Pig loader that uses specified character as a field delimiter.
	     *
	     * @param delimiter
	     *            the single byte character that is used to separate fields.
	     *            ("\t" is the default.)
	     */
	    public PigLoadUDF(String delimiter) {
	        this();
	        if (delimiter.length() == 1) {
	            this.fieldDel = (byte)delimiter.charAt(0);
	        } else if (delimiter.length() >  1 && delimiter.charAt(0) == '\\') {
	            switch (delimiter.charAt(1)) {
	            case 't':
	                this.fieldDel = (byte)'\t';
	                break;

	            case 'x':
	               fieldDel =
	                    Integer.valueOf(delimiter.substring(2), 16).byteValue();
	               break;

	            case 'u':
	                this.fieldDel =
	                    Integer.valueOf(delimiter.substring(2)).byteValue();
	                break;

	            default:
	                throw new RuntimeException("Unknown delimiter " + delimiter);
	            }
	        } else {
	            throw new RuntimeException("PigStorage delimeter must be a single character");
	        }
	    }

	    @Override
	    public Tuple getNext() throws IOException {
	        try {
	            boolean notDone = in.nextKeyValue();
	            if (notDone) {
	                return null;
	            }
	            Text value = (Text) in.getCurrentValue();
	            byte[] buf = value.getBytes();
	            int len = value.getLength();
	            int start = 0;

	            for (int i = 0; i < len; i++) {
	                if (buf[i] == fieldDel) {
	                    readField(buf, start, i);
	                    start = i + 1;
	                }
	            }
	            // pick up the last field
	            readField(buf, start, len);

	            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
	            mProtoTuple = null;
	            return t;
	        } catch (InterruptedException e) {
	            int errCode = 6018;
	            String errMsg = "Error while reading input";
	            throw new ExecException(errMsg, errCode,
	                    PigException.REMOTE_ENVIRONMENT, e);
	        }

	    }

	    private void readField(byte[] buf, int start, int end) {
	        if (mProtoTuple == null) {
	            mProtoTuple = new ArrayList<Object>();
	        }

	        if (start == end) {
	            // NULL value
	            mProtoTuple.add(null);
	        } else {
	            mProtoTuple.add(new DataByteArray(buf, start, end));
	        }
	    }

	    @Override
	    public InputFormat getInputFormat() {
	        return new TextInputFormat();
	    }

	    @Override
	    public void prepareToRead(RecordReader reader, PigSplit split) {
	        in = reader;
	    }

	    @Override
	    public void setLocation(String location, Job job)
	            throws IOException {
	        FileInputFormat.setInputPaths(job, location);
	    }

	// private static final String DELIM = "\t";
	// private static final int DEFAULT_LIMIT = 226;
	// private int limit = DEFAULT_LIMIT;
	// private RecordReader reader;
	// private List indexes;
	// private TupleFactory tupleFactory;
	//
	// /**
	// * Pig Loaders only take string parameters. The CTOR is really the only
	// interaction
	// * the user has with the Loader from the script.
	// * @param indexesAsStrings
	// */
	// public PigLoadUDF(String...indexesAsStrings) {
	// this.indexes = new ArrayList();
	// for(String indexAsString : indexesAsStrings) {
	// indexes.add(new Integer(indexAsString));
	// }
	//
	// tupleFactory = TupleFactory.getInstance();
	// }
	//
	//
	// @Override
	// public InputFormat getInputFormat() throws IOException {
	// return new TextInputFormat();
	//
	// }
	//
	// /**
	// * the input in this case is a TSV, so split it, make sure that the
	// requested indexes are valid,
	// */
	// @Override
	// public Tuple getNext() throws IOException {
	// Tuple tuple = null;
	// List values = new ArrayList();
	//
	// try {
	// boolean notDone = reader.nextKeyValue();
	// if (!notDone) {
	// return null;
	// }
	// Text value = (Text) reader.getCurrentValue();
	//
	// if(value != null) {
	// String parts[] = value.toString().split(DELIM);
	//
	// for(Integer index : indexes) {
	// if(index > limit) {
	// throw new IOException("index "+index+
	// "is out of bounds: max index = "+limit);
	// } else {
	// values.add(parts[index]);
	// }
	// }
	//
	// tuple = tupleFactory.newTuple(values);
	// }
	//
	// } catch (InterruptedException e) {
	// // add more information to the runtime exception condition.
	// int errCode = 6018;
	// String errMsg = "Error while reading input";
	// throw new ExecException(errMsg, errCode,
	// PigException.REMOTE_ENVIRONMENT, e);
	// }
	//
	// return tuple;
	//
	// }
	//
	// @Override
	// public void prepareToRead(RecordReader reader, PigSplit pigSplit)
	// throws IOException {
	// this.reader = reader; // note that for this Loader, we don't care about
	// the PigSplit.
	// }
	//
	// @Override
	// public void setLocation(String location, Job job) throws IOException {
	// FileInputFormat.setInputPaths(job, location); // the location is assumed
	// to be comma separated paths.
	//
	// }

}
