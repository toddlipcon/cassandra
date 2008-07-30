/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.infrastructure.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.log4j.Logger;

import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.db.ColumnFamily;
import com.facebook.infrastructure.db.IColumn;
import com.facebook.infrastructure.db.RowMutation;
import com.facebook.infrastructure.db.Table;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.gms.FailureDetector;
import com.facebook.infrastructure.gms.Gossiper;
import com.facebook.infrastructure.net.*;
import com.facebook.infrastructure.net.http.HTMLFormatter;
import com.facebook.infrastructure.net.http.HttpConnection;
import com.facebook.infrastructure.net.http.HttpRequest;
import com.facebook.infrastructure.net.http.HttpWriteResponse;
import com.facebook.infrastructure.utils.LogUtil;

/*
 * This class handles the incoming HTTP request after
 * it has been parsed.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class HttpRequestVerbHandler implements IVerbHandler
{
    private static final Logger logger_ = Logger.getLogger(HttpRequestVerbHandler.class);
    /* These are the list of actions supported */
    private static final String DETAILS = "details";
    private static final String LOADME = "loadme";
    private static final String KILLME = "killme";
    private static final String COMPACTME = "compactme";
    private static final String LB_HEALTH_CHECK = "lb_health_check";
    private static final String LB_HEALTH_CHECK_RESPONSE = "I-AM-ALIVE";
    private static final String QUERY = "query";
    private static final String INSERT = "insert";
    private static final String QUERYRESULTSDIV = "queryResultsDiv";
    private static final String INSERTRESULTSDIV = "insertResultsDiv";
    private static final String JS_UPDATE_QUERY_FUNCTION = "updateQueryResults";
    private static final String JS_UPDATE_INSERT_FUNCTION = "updateInsertResults";

    private StorageService storageService_;

    public HttpRequestVerbHandler(StorageService storageService)
    {
        storageService_ = storageService;
    }

    public void doVerb(Message message)
    {
        HttpConnection.HttpRequestMessage httpRequestMessage = (HttpConnection.HttpRequestMessage)message.getMessageBody()[0];
        try
        {
            HttpRequest httpRequest = httpRequestMessage.getHttpRequest();
            HttpWriteResponse httpServerResponse = new HttpWriteResponse(httpRequest);
            if(httpRequest.getMethod().toUpperCase().equals("GET"))
            {
                // handle the get request type
                doGet(httpRequest, httpServerResponse);
            }
            else if(httpRequest.getMethod().toUpperCase().equals("POST"))
            {
                // handle the POST request type
                doPost(httpRequest, httpServerResponse);
            }

            // write the response we have constructed into the socket
            ByteBuffer buffer = httpServerResponse.flush();
            httpRequestMessage.getHttpConnection().write(buffer);
        }
        catch(Exception e)
        {
            System.out.println("[onRequest] Exception: " + e);
        }
    }

    private void doGet(HttpRequest httpRequest, HttpWriteResponse httpResponse)
    {
        boolean fServeSummary = true;
        HTMLFormatter formatter = new HTMLFormatter();
        String query = httpRequest.getQuery();
        /*
         * we do not care about the path for most requests except those
         * from the load balancer
         */
        String path = httpRequest.getPath();
        /* for the health checks, just return the string only */
        if(path.indexOf(LB_HEALTH_CHECK) != -1)
        {
        	httpResponse.println(handleLBHealthCheck());
            return;
        }

        formatter.startBody(true, getJSFunctions(), true, true);
        formatter.appendLine("<h1><font color=\"white\"> Cluster map </font></h1>");

        StringBuilder sbResult = new StringBuilder();
        do
        {
            if(query.indexOf(DETAILS) != -1)
            {
                fServeSummary = false;
                sbResult.append(handleNodeDetails());
                break;
            }
            else if(query.indexOf(LOADME) != -1)
            {
                sbResult.append(handleLoadMe());
                break;
            }
            else if(query.indexOf(KILLME) != -1)
            {
                sbResult.append(handleKillMe());
                break;
            }
            else if(query.indexOf(COMPACTME) != -1)
            {
                sbResult.append(handleCompactMe());
                break;
            }
        }
        while(false);

        //formatter.appendLine("<br>-------END DEBUG INFO-------<br><br>");

        if(fServeSummary)
        {
            formatter.appendLine(handlePageDisplay(null, null));
        }

        formatter.appendLine("<br>");

        if(sbResult.toString() != null)
        {
            formatter.appendLine(sbResult.toString());
        }

        formatter.endBody();
        httpResponse.println(formatter.toString());
    }

    /*
     * As a result of the POST query, we currently only send back some
     * javascript that updates the data in some place on the browser.
    */
    private void doPost(HttpRequest httpRequest, HttpWriteResponse httpResponse)
    {
        String query = httpRequest.getQuery();

        HTMLFormatter formatter = new HTMLFormatter();
        formatter.startBody(true, getJSFunctions(), true, true);
        formatter.appendLine("<h1><font color=\"white\"> Cluster map </font></h1>");

        // write a shell for adding some javascript to do in-place updates
        StringBuilder sbResult = new StringBuilder();
        do
        {
            if(query.indexOf(QUERY) != -1)
            {
                sbResult.append(handleQuery(httpRequest));
                break;
            }
            else if(query.indexOf(INSERT) != -1)
            {
                sbResult.append(handleInsert(httpRequest));
                break;
            }
        }
        while(false);

        if(sbResult.toString() != null)
        {
            formatter.appendLine(sbResult.toString());
        }

        formatter.endBody();

    	httpResponse.println(formatter.toString());
    }

    private String handleNodeDetails()
    {
        HTMLFormatter formatter = new HTMLFormatter();

        formatter.appendLine("Token: " + storageService_.getToken());
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        formatter.appendLine("Up time (in seconds): " + (runtimeMxBean.getUptime()/1000));

        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memUsage = memoryMxBean.getHeapMemoryUsage();
        java.text.DecimalFormat df = new java.text.DecimalFormat("#0.00");
        String smemUsed = df.format((double)memUsage.getUsed()/(1024 * 1024));
        String smemMax = df.format((double)memUsage.getMax()/(1024 * 1024));
        formatter.appendLine("Heap memory usage (in MB): " + smemUsed + "/" + smemMax);

        formatter.appendLine("<br>");
        formatter.appendLine("<br>");

        /*
         * Display DB statatics if we have something to show.
        */
        displayDBStatistics(formatter, df);

        formatter.appendLine("<button onClick=\"window.location='" + StorageService.getHostUrl() + "?" + LOADME + "=T'\">Load Me</button>");
        formatter.appendLine("<button onClick=\"window.location='" + StorageService.getHostUrl() + "?" + COMPACTME + "=T'\">Compact Me</button>");
        formatter.appendLine("<button onClick=\"window.location='" + StorageService.getHostUrl() + "?" + KILLME + "=T'\">Kill Me</button>");

        formatter.appendLine("<br>");
        formatter.appendLine("<br><a href='" + StorageService.getHostUrl() + "'>Back to live nodes list" + "</a>");

        return formatter.toString();
    }

    private void displayDBStatistics(HTMLFormatter formatter, java.text.DecimalFormat df)
    {
        String tableStats = Table.open( DatabaseDescriptor.getTables().get(0) ).tableStats("\n<br>\n", df);

        if ( tableStats.length() == 0 )
            return;

        formatter.appendLine("DB statistics:");
        formatter.appendLine("<br>");
        formatter.appendLine("<br>");

        formatter.appendLine(tableStats);
        formatter.appendLine("<br>");
        formatter.appendLine("<br>");
    }

    private String handlePageDisplay(String queryFormData, String insertFormData)
    {
    	StringBuilder sb = new StringBuilder();
		sb.append("\n<div id=\"header\"> \n");
		sb.append("<ul>\n");
		sb.append("	<li name=\"one\" onclick=\"javascript:selectTab('one')\"><a href=\"#\">Cluster</a>\n");
		sb.append("	</li>\n");
		sb.append("	<li name=\"two\" onclick=\"javascript:selectTab('two')\"><a href=\"#\">SQL</a></li>\n");
		sb.append("	<li name=\"three\" onclick=\"javascript:selectTab('three')\"><a href=\"#\">Ring</a></li>\n");
		sb.append("</ul>\n");
		sb.append("</div>\n\n");

		sb.append("<div name=\"one\" id=\"content\"> <!-- start tab one -->\n\n");
        sb.append(serveSummary());
        sb.append("</div> <!-- finish tab one -->\n\n");

        sb.append("<div name=\"two\" id=\"content\"> <!-- start tab two -->\n\n");
        sb.append(serveInsertForm(insertFormData));
        sb.append(serveQueryForm(queryFormData));
        sb.append("</div> <!-- finish tab two -->\n\n");

        sb.append("<div name=\"three\" id=\"content\"> <!-- start tab three -->\n\n");
        sb.append(serveRingView());
        sb.append("</div> <!-- finish tab three -->\n\n");

        sb.append("\n<script type=\"text/javascript\">\n");
        if(queryFormData != null || insertFormData != null)
        	sb.append("selectTab(\"two\");\n");
        else
        	sb.append("selectTab(\"one\");\n");

        sb.append("</script>\n");

        return (sb.toString() == null)?"":sb.toString();
    }

    /*
     * Serve the summary of the current node.
     */
    private String serveSummary()
    {
        HTMLFormatter formatter = new HTMLFormatter();

        Set<EndPoint> liveNodeList = Gossiper.instance().getAllMembers();
        // we want this set of live nodes sorted based on the hostname
        EndPoint[] liveNodes = liveNodeList.toArray(new EndPoint[0]);
        Arrays.sort(liveNodes);

        String[] sHeaders = {"Node No.", "Host:Port", "Status", "Leader", "Load Info", "Token", "Generation No."};
        formatter.startTable();
        formatter.addHeaders(sHeaders);
        int iNodeNumber = 0;
        for( EndPoint curNode : liveNodes )
        {
            formatter.startRow();
            ++iNodeNumber;

            // Node No.
            formatter.addCol("" + iNodeNumber);
            // Host:Port
            formatter.addCol("<a href='http://" + curNode.getHost() + ":" + DatabaseDescriptor.getHttpPort() + "/home?" + DETAILS + "=T'>" + curNode.getHost() + ":" + curNode.getPort() + "</a>");
            //Status
            String status = ( FailureDetector.instance().isAlive(curNode) ) ? "Up" : "Down";
            formatter.addCol(status);
            //Leader
            // 20080716: Hardcode "isLeader = false;" until Zookeeper integration is complete (jeff.hammerbacher)
            // boolean isLeader = StorageService.instance().isLeader(curNode);
            boolean isLeader = false;
            formatter.addCol(Boolean.toString(isLeader));
            //Load Info
            String loadInfo = getLoadInfo(curNode);
            formatter.addCol(loadInfo);
            // Token
            if(curNode == null)
                formatter.addCol("NULL!");
            else
                formatter.addCol(storageService_.getToken(curNode));
            // Generation Number
            formatter.addCol(Integer.toString(Gossiper.instance().getCurrentGenerationNumber(curNode)));

            formatter.endRow();
        }

        formatter.endTable();

        return formatter.toString();
    }

    private String serveRingView()
    {
        HTMLFormatter formatter = new HTMLFormatter();
        String[] sHeaders = {"Range No.", "Range", "N1", "N2", "N3"};
        formatter.startTable();
        formatter.addHeaders(sHeaders);

        Map<Range, List<EndPoint>> oldRangeToEndPointMap = StorageService.instance().getRangeToEndPointMap();
        Set<Range> rangeSet = oldRangeToEndPointMap.keySet();

        int iNodeNumber = 0;
        for ( Range range : rangeSet )
        {
        	formatter.startRow();
            ++iNodeNumber;

            // Range No.
            formatter.addCol("" + iNodeNumber);

            // Range
            formatter.addCol("(" + range.left() + ",<br>" + range.right() + "]");

            List<EndPoint> replicas = oldRangeToEndPointMap.get(range);
            for ( EndPoint replica : replicas )
            {
            	// N1 N2 N3
            	formatter.addCol(replica.toString());
            }

            formatter.endRow();
        }

        formatter.endTable();

        return formatter.toString();
    }

    private String getLoadInfo(EndPoint ep)
    {
        if ( StorageService.getLocalControlEndPoint().equals(ep) )
        {
            return StorageService.instance().getLoadInfo();
        }
        else
        {
            return StorageService.instance().getLoadInfo(ep);
        }        
    }

    /*
     * Returns the HTML code for a form to query data from the db cluster.
     */
    private String serveQueryForm(String queryResult)
    {
        HTMLFormatter formatter = new HTMLFormatter();
        formatter.appendLine("<BR><fieldset><legend>Query the cluster</legend>");
        formatter.appendLine("<FORM action=\"" + StorageService.getHostUrl() + "/home?" + QUERY + "=T\" method=\"post\">");

        // get the list of column families
        Table table = Table.open("Mailbox");
        Set<String> columnFamilyComboBoxSet = table.getColumnFamilies();

        formatter.append("select from ");
        formatter.addCombobox(columnFamilyComboBoxSet, "columnfamily", 0);
        formatter.append(" : <INPUT name=columnName>");
        formatter.appendLine(" where key = <INPUT name=key>");
        formatter.appendLine("<BR>");
        formatter.appendLine("<INPUT type=\"submit\" value=\"Send\"> <INPUT type=\"reset\">");

        formatter.appendLine("</FORM>");
        formatter.addDivElement(QUERYRESULTSDIV, queryResult);
        formatter.appendLine("</fieldset><BR>");

        return formatter.toString();
    }

    /*
     * Handle the query of some data from the client.
     */
    private String handleQuery(HttpRequest httpRequest)
    {
    	boolean fQuerySuccess = false;
    	String sRetVal = "";

    	// get the various values for this HTTP request
    	String sColumnFamily = httpRequest.getParameter("columnfamily");
    	String sColumn = httpRequest.getParameter("columnName");
    	String sKey = httpRequest.getParameter("key");

    	// get the table name
    	String sTableName = DatabaseDescriptor.getTables().get(0);

        try
        {
	    	Table table = Table.open(sTableName);
	    	String queryFor = sColumnFamily;
	    	if(sColumn != null && !"*".equals(sColumn))
    		{
	    		queryFor += ":" + sColumn;
    		}
	        ColumnFamily cf = table.get(sKey, queryFor);

	        if (cf == null)
	        {
	            sRetVal = "Key [" + sKey + "], column family [" + sColumnFamily + "] not found.";
	        }
	        else
	        {
	        	String columnFamilyType = DatabaseDescriptor.getColumnType(cf.name());
	        	if("Super".equals(columnFamilyType))
	        	{
	        		// print the super column
	        		sRetVal = printSuperColumns(cf, sColumn, sKey);
	        	}
	        	else
	        	{
	        		sRetVal = printColumnFamily(cf, sColumn, sKey);
	        	}

	        	fQuerySuccess = true;

	        }
        }
        catch (Exception e)
        {
        	// write failed - return the reason
        	sRetVal = e.getMessage();
        }

        if(fQuerySuccess)
        	sRetVal = "Success: " + sRetVal;
        else
        	sRetVal = "Error: " + sRetVal;


        return handlePageDisplay(sRetVal, null);
    }

    private String printSuperColumns(ColumnFamily cf, String sColumnQuery, String sKey)
    {
    	String superColumnName = "*";
    	String columnName = "*";
    	String sRetVal = "";
    	// the query can be in one of the following forms:
    	// * - meaning print all super columns and all columns underneath them
    	// sc - same as sc:*
    	// sc:* - print all columns under the supercolumn sc
    	// sc:cn - print column name cn under super-colummn sc
    	if("*".equals(sColumnQuery))
    	{
    		// print everything
    	}
    	else if(sColumnQuery.indexOf(':') == -1)
    	{
    		// specific super-col, all the columns
    		superColumnName = sColumnQuery;
    	}
    	else
    	{
    		String[] sc_cn = sColumnQuery.split(":");
    		superColumnName = sc_cn[0];
    		columnName = sc_cn[1];
    		sRetVal = "Query for " + sc_cn + " returned with success.";
    	}

    	return sRetVal;
    }

    private String printColumnFamily(ColumnFamily cf, String sColumn, String sKey)
    {
    	StringBuffer sb = new StringBuffer();

    	if("*".equals(sColumn))
    	{
    		Collection<IColumn> columnCollection = cf.getSortedColumns();
    		sb.append("Key = " + sKey + "<br>");
    		// draw the table out
    		HTMLFormatter formatter = new HTMLFormatter();
    		int numColumns = columnCollection.size();
    		String[] columnNames = new String[numColumns];
    		String[] columnValues = new String[numColumns];
    		IColumn[] icolumnArray = columnCollection.toArray(new IColumn[0]);

    		for(int i = 0; i <  numColumns; ++i)
    		{
    			columnNames[i] = icolumnArray[i].name();
    			columnValues[i] = new String(icolumnArray[i].value());
    		}

    		formatter.startTable();
            formatter.addHeaders(columnNames);

			formatter.startRow();
    		for(int i = 0; i <  numColumns; ++i)
    		{
    			formatter.addCol(columnValues[i]);
    		}
			formatter.endRow();
    		formatter.endTable();

    		sb.append(formatter.toString());
    	}
    	else
    	{
    		IColumn column = cf.getColumn(sColumn);
    		// print the name of this column
    		sb.append("Key = " + sKey + " Column = " + sColumn + " value = " + (new String(column.value())) + "<br>");
    	}

    	return sb.toString();
    }


    /*
     * Returns the HTML code for a form to insert data into the db cluster.
     */
    private String serveInsertForm(String insertResult)
    {
        HTMLFormatter formatter = new HTMLFormatter();
        formatter.appendLine("<BR><fieldset>\n<legend>Insert data into the cluster</legend>\n");
        formatter.appendLine("<FORM action=\"" + StorageService.getHostUrl() + "/home?" + INSERT + "=T\" method=\"post\">");

        // get the list of column families
        Table table = Table.open("Mailbox");
        Set<String> columnFamilyComboBoxSet = table.getColumnFamilies();

        formatter.append("insert into ");
        formatter.addCombobox(columnFamilyComboBoxSet, "columnfamily", 0);
        formatter.append(" : <INPUT name=columnName>");
        formatter.append(" data = <INPUT name=data>");
        formatter.appendLine(" where key = <INPUT name=key>\n");
        formatter.appendLine("<BR>\n");
        formatter.appendLine("<INPUT type=\"submit\" value=\"Send\"> <INPUT type=\"reset\">\n");

        formatter.appendLine("</FORM>\n");
        formatter.addDivElement(INSERTRESULTSDIV, insertResult);
        formatter.appendLine("</fieldset>\n<BR>\n");

        return formatter.toString();
    }

    /*
     * Handle the query of some data from the client.
     */
    private String handleInsert(HttpRequest httpRequest)
    {
    	boolean fInsertSuccess = false;
    	String sRetVal = "";

    	// get the various values for this HTTP request
    	String sColumnFamily = httpRequest.getParameter("columnfamily");
    	String sColumn = httpRequest.getParameter("columnName");
    	String sKey = httpRequest.getParameter("key");
    	String sDataToInsert = httpRequest.getParameter("data");

    	// get the table name
    	String sTableName = DatabaseDescriptor.getTables().get(0);

        try
        {
        	// do the insert first
            RowMutation rm = new RowMutation(sTableName, sKey);
            rm.add(sColumnFamily + ":" + sColumn, sDataToInsert.getBytes(), 0);
            rm.apply();

            fInsertSuccess = true;
	        sRetVal = "columnfamily=" + httpRequest.getParameter("columnfamily") + " key=" + httpRequest.getParameter("key") + " data=" + httpRequest.getParameter("data");
        }
        catch (Exception e)
        {
        	// write failed - return the reason
        	sRetVal = e.getMessage();
        }
        System.out.println("Write done ...");

        if(fInsertSuccess)
        	sRetVal = "The insert was successful : " + sRetVal;
        else
        	sRetVal = "The insert was failed : " + sRetVal;

        return handlePageDisplay(null, sRetVal);
    }


    private String getJSFunctions()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("function " + JS_UPDATE_QUERY_FUNCTION + "(text)\n");
        sb.append("{\n");
        sb.append("    obj = document.getElementById(\"" + QUERYRESULTSDIV + "\");\n");
        sb.append("    if(obj)\n");
        sb.append("        obj.innerHTML = text;\n");
        sb.append("}\n");
        sb.append("\n");
        sb.append("function " + JS_UPDATE_INSERT_FUNCTION + "(text)\n");
        sb.append("{\n");
        sb.append("    obj = document.getElementById(\"" + INSERTRESULTSDIV + "\");\n");
        sb.append("    if(obj)\n");
        sb.append("        obj.innerHTML = text;\n");
        sb.append("}\n");
        sb.append("\n");

        return sb.toString();
    }

    /*
     * Load the current node with data.
     */
    private String handleLoadMe()
    {        
        return "Loading...";
    }

    private String handleCompactMe()
    {
        Table table = Table.open(DatabaseDescriptor.getTables().get(0));
        try
        {
            table.forceCompaction();
        }
        catch (IOException ex)
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
        return "Compacting ...";
    }

    private String handleLBHealthCheck()
    {
    	if(StorageService.instance().isShutdown())
    		return "";
    	return LB_HEALTH_CHECK_RESPONSE;
    }

    /*
     * Kill the current node.
     */
    private String handleKillMe()
    {
    	if(StorageService.instance().isShutdown())
    		return "Already scheduled for being shutdown";
    	/*
    	 * The storage service will wait for a period of time to let the
    	 * VIP know that we are shutting down, then will perform an actual
    	 * shutdown on a separate thread.
    	 */
        String status = "Service has been killed";
        try
        {
            StorageService.instance().killMe();
        }
        catch( Throwable th )
        {
            logger_.warn(LogUtil.throwableToString(th));
            status = "Failed to kill service.";
        }
    	return status;
    }

}
