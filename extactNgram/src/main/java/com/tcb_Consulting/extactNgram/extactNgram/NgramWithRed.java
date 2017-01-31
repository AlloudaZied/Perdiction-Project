package com.tcb_Consulting.extactNgram.extactNgram;

import java.util.ArrayList;
import java.util.List;



public class NgramWithRed implements ExtractNgram {
   public static void main(String[] args) {
		NgramWithRed ng = new NgramWithRed();
		List<String> list = null ;
        String str = "le plus anion vole";
        list = ng.extractNgram(3, str);
     for (String st :list)
         {    	 
    	 if (!list.isEmpty())
    	 System.out.println(st);
         }
	   }
         /**
          * Cleaning NGRAM
          * @param tweets  : the message to be process
          ***/
    public List<String> extractNgram(int N, String sent) {
		 String[] tokens = sent.split(" "); 
         List<String> list = new ArrayList<String>();
      for(int k=0; k<(tokens.length-N+1); k++){
         String s="";
         int start=k;
         int end=k+N;
      for(int j=start; j<end; j++){
           s=s+" "+tokens[j];
                  }    
           s=processTweets(s);
           list.add(s);
           s = s+"\n";
  }
     return list;
  } 
       /**
        * this method generate an NGRAM from giving text
        * @param N : number of gram
        * @param sent : the sentences to be processed
        ***/
public static String processTweets(String tweets)
	   {
	  	String []splitted = tweets.split(" ");
	  	String tweetsprocessed = "";
	  	for (String word : splitted)
	  	     {
	     	tweetsprocessed =tweetsprocessed +" "+word.replaceAll("#", "HASHTAG");	
	         }	
	  	return tweetsprocessed ;
	  }
}
