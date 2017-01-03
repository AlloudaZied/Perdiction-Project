package org.tcbConsulting.yarn;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
/** 
 * The ApplicationMaster program implement an application that 
 * run the submitted jar  from client in the Containers
 * PS:the number of container for running should be available
 * @author SENDI ZIED
 * @version 1.0 
 * @since 22-12-2016
 * */
public class ApplicationMaster {
	final static Logger logger = Logger.getLogger(ApplicationMaster.class);
	
	/**
     * This method is the main method to run a Client yarn.
     * 
     * @param args[0] 
     *            This is the number of container
     * @param args[1]  
     *            This is the list of the brokers
     * @param args[2]  
     *            This is the name of the topic 
     * @throws Exception    
     * */	
	public static void main(String[] args) throws Exception {
        final int n = Integer.valueOf(args[0]);
        final String brokerlist =     args[1];
        final String topicName =      args[2] ;
        Map <Integer,String> startKey = new HashMap<Integer,String>();
        Map <Integer,String> stopKey  = new HashMap<Integer,String>(); 
        startKey.put(1,"1");
        startKey.put(2,"3000000");
        startKey.put(3,"5000000");
        stopKey.put (1,"3000000");
        stopKey.put (2,"5000000");
        stopKey.put (3,"6000000"); 
        logger.info("Running ApplicationMaster!");
        Configuration conf = new YarnConfiguration();
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();
        logger.info("register Application Master 0");
        rmClient.registerApplicationMaster("", 0, "");
        logger.info("register Application Master 1");
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        Path jarPath = new Path("/user/sendi/apps/Exemple-Produce-on-kafka-0.0.1-SNAPSHOT-jar-with-dependencies.jar");
        jarPath = FileSystem.get(conf).makeQualified(jarPath);
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupContainerJar(conf, jarPath, appMasterJar);
        Map<String, String> containerEnv = new HashMap<String, String>();
        setupContainerEnv(conf, containerEnv);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
//        capability.setVirtualCores(1);
//        for (int i = 0; i < n; ++i) {
//            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
//            logger.info("Making request for container : " + i);
//            rmClient.addContainerRequest(containerAsk);
//        }       
        requestForContainer( n, rmClient,capability, priority);
        int task = 1;
        int allocatedContainers = 0;
        // We need to start counting completed containers while still allocating
        // them since initial ones may complete while we're allocating subsequent
        // containers and if we miss those notifications, we'll never see them again
        // and this ApplicationMaster will hang indefinitely.
        int completedContainers = 0;
        while (task<3)
        while (allocatedContainers < n) {
            AllocateResponse response = rmClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                ++allocatedContainers;
                ContainerLaunchContext appContainer = Records.newRecord(ContainerLaunchContext.class);
                appContainer.setLocalResources(Collections.singletonMap("simpleapp.jar", appMasterJar));
                appContainer.setEnvironment(containerEnv);
                appContainer.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java" +
                                        " -Xmx256M" +
                                        " org.java.twitter.Exemple_Produce_on_kafka.ProducerKafka" +
                                        " "+
                                        startKey.get(task)+
                                        " "+
                                        stopKey.get(task) +
                                        " "+ brokerlist +
                                        " "+ topicName +
                                        " "+
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        )
                );
                nmClient.startContainer(container, appContainer);
                ++task;
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
               ++completedContainers;
               logger.info("Completed container " + completedContainers + status);
            }
            Thread.sleep(100);
        }
        while (completedContainers < n) {
            AllocateResponse response = rmClient.allocate(completedContainers / n);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                logger.info("Completed container " + completedContainers + status);
            }
       Thread.sleep(100);}
       rmClient.unregisterApplicationMaster(
       FinalApplicationStatus.SUCCEEDED, "", "");
       logger.info("Finished");
    }
	/**
     * This method is used to set the jar for the container.
     * 
     * @param conf  
     *           HADOOP configuration
     * @param jarPath      
     *           This is the path of the jar
     * @param appMasterJar  
     *           This is the path of the local resource (jar,configuration file ...)
     * @throws IOException
     */   
    private static void setupContainerJar(Configuration conf, Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }
    /**
     * This method is used to set the ENV of the running.
     * @param appMasterEnv    This is the map of the ENV set up
     */ 
    private static void setupContainerEnv(Configuration conf, Map<String, String> containerEnv) {
        String classPathEnv = "./*";
        containerEnv.put("CLASSPATH", classPathEnv);
        String[] defaultYarnAppClasspath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
        System.out.println("*** YARN_APPLICATION_CLASSPATH: " +
        Arrays.asList(defaultYarnAppClasspath != null ? defaultYarnAppClasspath : new String[]{}));
        logger.info("*** APP CONTAINER ENV: " +containerEnv);
    }
    /**
     * This method is used to ask for number of container from resource manager.
     * 
     * @param numberofcontainer    
     *                  This is the number of container to ask
     * @param rmClient             
     *                  This is the connector to resource manager
     * @param capability          
     *                  This is the number the capability of requested container
     * @param priority             
     *                  This is the priority of the application (intern application)
     */ 
    private static void requestForContainer(int numberofcontainer,AMRMClient<ContainerRequest> rmClient,Resource capability,Priority priority)
    { 
    	for (int i = 0 ;i<numberofcontainer;++i)
    	{
    		ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
    		logger.info("Making request for container : " + i);
            rmClient.addContainerRequest(containerAsk);
    	}
        
    }

}