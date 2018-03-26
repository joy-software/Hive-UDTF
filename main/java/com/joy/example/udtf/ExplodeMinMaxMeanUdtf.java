package com.joy.example.udtf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


/**
 * 
 * @author ZVJN1964
 *
 */
@Description(name = "explodeMean",
value = "_FUNC_(a) - give the min, max and mean of the elements of an array")
public class ExplodeMinMaxMeanUdtf extends GenericUDTF {

	// ObjectInspector for our input data
	//transient mean i don't want to serialize this variable if we may to serialize an object of this class
	private transient ListObjectInspector loi;
	//PrimitiveObjectInpector to perform the mean calculation
	//over the received data
	private transient PrimitiveObjectInspector poi;


	public StructObjectInspector initialiaze(ObjectInspector[] parameters) throws UDFArgumentException
	{
		if (parameters.length != 1) {
			throw new UDFArgumentException("explodeMean() takes only one argument");
		}

		//those two variables will be used to construct our return structures
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		//Get the object Inpector of our list
		ObjectInspector oi = parameters[0];

		//check if the input is a list
		switch(oi.getCategory())
		{
		case LIST:
			break;
		default:
			throw new UDFArgumentTypeException(0,
					"Argument must be a list (array)"
							+ oi.getCategory()
							+ " was passed.");
		}

		//Transform it into listObjectInspector
	    loi = ((ListObjectInspector)oi);


		//Verify that the data inside the array are primitive
		if(loi.getListElementObjectInspector().getCategory() != Category.PRIMITIVE)
		{
			throw new UDFArgumentTypeException(0, "Array Must contains only primitive Type");
		}

		//Get the primitive ObjectInspector of our input data
		poi = (PrimitiveObjectInspector) loi.getListElementObjectInspector();

		//check if we can compute a mean on the data we have received
		//Only double, float and int are accepted
		switch(poi.getPrimitiveCategory())
		{
		case DOUBLE:
			break;
		case FLOAT:
			break;
		case INT:
			break;
		default:
			throw new UDFArgumentTypeException(0,
					"Argument must be double, float or int, but "
							+ poi.getPrimitiveCategory()
							+ " was passed.");
		}


		
		
		//Add the last field for our mean
		fieldNames.add("Min");
		fieldNames.add("Max");
		fieldNames.add("Mean");
		fieldOIs.add(poi);
		fieldOIs.add(poi);
		fieldOIs.add(poi);


		//Return a structure Object Inspector based on that fieldNames and fieldOIs
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
		        fieldOIs);
	}



	@Override
	public void process(Object[] args) throws HiveException {
		
		assert(args.length == 1);
		
		 List<?> list = loi.getList(args[0]);
	      if (list == null) {
	        return;
	      }
	      
	      Object[] temp = new Object[3];
	      
	      //iterate over the list of object contains inside our input data
	      int i = 0;
	      for (Object object : args) {
	        temp[i] = object;
	      }
	      //Forward the result to the collector
	      forward(minMaxMean(temp));
	}

	/**
	 * Perform of the calculation of the min, max and mean
	 * @param args
	 * @return
	 */
	public Double[] minMaxMean(Object[] args) {
		//Create an object that will contains the temporary result of each input
	      Double[] forwardListObj = new Double[3];
	      
	      //sum will be used to computed the mean calculation
	      Double sum = 0.0;
	      Double min = Double.MAX_VALUE, max=Double.MIN_VALUE;
	      
	      //iterate over the list of object contains inside our input data
	      int i = 0;
	      for (Object object : args) {
	        Double temp = Double.parseDouble(""+poi.getPrimitiveJavaObject(object));
	        
	        if(temp < min)
	        {
	        	min = temp;
	        }
	        if(temp > max)
	        {
	        	max = temp;
	        }
	        sum += temp;
	        i++;
	      }
	      
	      
	      
	      //Store the result
	      forwardListObj[0] = min;
	      forwardListObj[1] = max;
	      forwardListObj[2] = sum/i;
	      
	      return forwardListObj;
	}



	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub

	}

}
