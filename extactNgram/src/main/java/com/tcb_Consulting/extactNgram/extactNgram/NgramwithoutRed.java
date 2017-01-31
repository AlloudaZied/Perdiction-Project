package com.tcb_Consulting.extactNgram.extactNgram;

import java.util.List;

   public class NgramwithoutRed implements ExtractNgram{
	public static void main(String[] args) {
		NgramWithRed ng = new NgramWithRed();
		List<String> list = null ;
        String str = "This release includes initial support"
 		+ " for running Spark against HBase "
 		+ "  with a richer feature set than was previously possible "
 		+ "with MapReduce bindings";
     list = ng.extractNgram(2, str);
        for (String st :list)
         {    	 
    	 System.out.println(st);
         }
	}
   public List<String> extractNgram(int N, String sent) {
	
	   return null;		
	}
}