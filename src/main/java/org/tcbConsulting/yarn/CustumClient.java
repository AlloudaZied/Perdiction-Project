package org.tcbConsulting.yarn;

import java.io.File;
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
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/** 
 * The CustumClient program implement an application that 
 * run Client Yarn ,the first step is to connect to the
 * resource manager and to submit the execution of Application
 * Master
 * @author SENDI ZIED
 * @version 1.0 
 * @since 22-12-2016
 * */
public class CustumClient {

    Configuration conf = new YarnConfiguration();
 
    /**
     * This method is used to run Yarn Client.The steps to run a 
     * yarn an Application Master with command shell in one container
     * @param args[0]  This is the number of container
     * @param args[1]  This is the list of the brokers
     * @param args[2]  This is the name of the topic
     */     
    public void run(String[] args) throws Exception {
        final int n = Integer.valueOf(args[0]);
        final String brokerlist =args[1];
        final String topicname = args[2];
        Path jarPath = new Path("/apps/simple/yarn-application-0.0.1-SNAPSHOT.jar");
        jarPath = FileSystem.get(conf).makeQualified(jarPath);
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        YarnClientApplication app = yarnClient.createApplication();
        ContainerLaunchContext amContainer =
                Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(
                Collections.singletonList(
                        "$JAVA_HOME/bin/java" +
                                " -Xmx256M" +
                                " org.tcbConsulting.yarn.ApplicationMaster" +
                                " " + String.valueOf(n) +
                                " " + brokerlist  +
                                " " + topicname +
                                " " +
                                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                )
        );
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupAppMasterJar(jarPath, appMasterJar);
        amContainer.setLocalResources(
                Collections.singletonMap("simpleapp.jar", appMasterJar));
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);
        ApplicationSubmissionContext appContext =app.getApplicationSubmissionContext();
        appContext.setApplicationName("tcb-yarn-application"); 
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); 
        ApplicationId appId = appContext.getApplicationId();
        yarnClient.submitApplication(appContext);
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }     
        yarnClient.stop();
        yarnClient.close();
    }    
    /**
     * This method is used to set the jar containing Application Master.
     * @param jarPath       This is the path of the jar
     * @param appMasterJar  This is the path of the local resource (jar,configuration file ...)
     * @throws IOException
     */     
    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
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
    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        String classPathEnv = "./*";
        appMasterEnv.put("CLASSPATH", classPathEnv);
        String[] defaultYarnAppClasspath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
        System.out.println("*** YARN_APPLICATION_CLASSPATH: " +Arrays.asList(defaultYarnAppClasspath != null ? defaultYarnAppClasspath : new String[]{}));
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                    c.trim(), File.pathSeparator);
        }
    }
    /**
     * This method is the main method to run a Client yarn.
     * @param args[0]  This is the number of container
     * @param args[1]  This is the list of the brokers
     * @param args[2]  This is the name of the topic     
     * */
    public static void main(String[] args) throws Exception {
        CustumClient c = new CustumClient();
        c.run(args);
    }
}