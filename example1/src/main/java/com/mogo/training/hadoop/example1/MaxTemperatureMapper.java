/** 
 * Copyright 2015 MoGo Mantra Inc <http://www.mogomantra.com> 
 *  
 * Created Mar 20, 2015 
 * 
 * Last edited by:      $Author: $  
 *             on:      $Date: $  
 *       Filename:      $Id: $ 
 *       Revision:      $Rev: $ 
 *            URL:      $URL: $ 
 */
package com.mogo.training.hadoop.example1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author mohangoyal
 * 
 */
public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		//extract year 
		String year = line.substring(15,19);
		
		//extract temperature
		int airTemperature;
		if(line.charAt(87) == '+') {
			airTemperature = Integer.parseInt(line.substring(88,92));
		}else {
			airTemperature = Integer.parseInt(line.substring(87,92));	
		}
		
		String quality = line.substring(92,93);
		if(airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year),  new IntWritable(airTemperature));
		}
		
	}

}
