package com.joy.example.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ExplodeMinMaxMeanUdtfTest {
	
	ExplodeMinMaxMeanUdtf explodeMean;
	
	private PrimitiveObjectInspector poi;
	private ObjectInspector[] input;


 
    Object[] param1 = {2.0, 4.0, 7.9, 2.1};
    Object[] param2 = {40, 20, 9, 11};
    
    
   @Before
    public void setUp() throws Exception {
    	
	   explodeMean = new ExplodeMinMaxMeanUdtf();
	   
	   poi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
			   PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
	   
      input = new ObjectInspector[1];
      input[0] = ObjectInspectorFactory.getStandardListObjectInspector(poi);
       
       explodeMean.initialiaze(input);
    }
    
    @After
    public void tearDown() throws Exception {

    }
    
    @Test (expected = UDFArgumentException.class)
    public void testInitializeWithMoreThanOneArgument() throws UDFArgumentException
    {
 	   ObjectInspector[] tpoi = new ObjectInspector[2];
        tpoi[0] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        tpoi[1] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
       explodeMean.initialiaze(tpoi);
    }

    @Test (expected = UDFArgumentTypeException.class)
    public void testInitializeWithBadCategory() throws UDFArgumentException
    {
 	   ObjectInspector[] tpoi = new ObjectInspector[1];
        tpoi[0] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
       explodeMean.initialiaze(tpoi);
    }
    
    @Test (expected = UDFArgumentTypeException.class)
    public void testInitializeWithArrayButBadPrimitiveCategory() throws UDFArgumentException
    {
 	   ObjectInspector[] tpoi = new ObjectInspector[1];
        tpoi[0] =  ObjectInspectorFactory.getStandardListObjectInspector(
        		ObjectInspectorFactory.getStandardMapObjectInspector(poi, poi));
       explodeMean.initialiaze(tpoi);
    }
    
    @Test
    public void testMinMaxMean() throws UDFArgumentException
    {
    	
    	explodeMean.initialiaze(input);
    	Double[] expected = {2.0, 7.9, 4.0};
    	Assert.assertArrayEquals(expected, explodeMean.minMaxMean(param1));
    	
    	Double[] expected2 = {9.0, 40.0, 20.0};
    	Assert.assertArrayEquals(expected2, explodeMean.minMaxMean(param2));
    }

}
