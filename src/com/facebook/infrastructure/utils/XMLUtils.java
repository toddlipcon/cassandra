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

package com.facebook.infrastructure.utils;

import java.util.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import java.io.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import org.apache.xpath.*;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class XMLUtils
{
	private Document document_;
	private Node rootNode_;

    public XMLUtils(String xmlSrc) throws FileNotFoundException, ParserConfigurationException, SAXException, IOException
    {
        FileInputStream fis = new FileInputStream(xmlSrc);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        document_ = db.parse(fis);
        rootNode_ = document_.getFirstChild();
        fis.close();
    }

	public XMLUtils(byte[] bytes) throws IOException, ParserConfigurationException, SAXException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        document_ = db.parse(bis);
        rootNode_ = document_.getFirstChild();
        bis.close();
    }

	public String getNodeValue(String xql) throws TransformerException
	{
        String value = null;
        Node node = XPathAPI.selectSingleNode( rootNode_, xql );
        if ( node != null )
        {
            node = node.getFirstChild();
            if ( node != null )
            {
                value = node.getNodeValue();
            }
        }
        return value;
	}

	public String getNodeValue(Node node, String xql) throws TransformerException
	{
        String value = null;
        Node nd = XPathAPI.selectSingleNode( node, xql );
        if ( nd != null )
        {
            nd = nd.getFirstChild();
            if ( nd != null )
            {
                value = nd.getNodeValue();
            }
        }
        return value;
	}

	public String[] getNodeValues(String xql) throws TransformerException
	{
		return getNodeValues(rootNode_, xql);
	}

	public String[] getNodeValues(Node n, String xql) throws TransformerException
	{
	    NodeList nl = getRequestedNodeList(n, xql);
	    int size = nl.getLength();
	    String[] values = new String[size];
	    
	    for ( int i = 0; i < size; ++i )
	    {
            Node node = nl.item(i);
            node = node.getFirstChild();
            values[i] = node.getNodeValue();
	    }
	    return values;
	}

    public Map getNodeValuesWithAttrs(String xql) throws TransformerException{
    	return getNodeValuesWithAttrs(rootNode_, xql);
    }

    public Map getNodeValuesWithAttrs(Node n, String xql) throws TransformerException
	{
	    NodeList nl = getRequestedNodeList(n, xql);
	    int size = nl.getLength();
	    Map value = new HashMap();

	    for ( int i = 0; i < size; ++i )
	    {
            Node node = nl.item(i);
            String attr = node.getAttributes().item(0).getNodeValue();             
            node = node.getFirstChild();
            List list = (List)value.get(attr);
            if ( list == null )
            {
                list = new ArrayList();
            }
            list.add(node.getNodeValue());
            value.put(attr, list);
	    }
	    return value;
	}

	public Node getRequestedNode(String xql) throws TransformerException
	{
		Node node = XPathAPI.selectSingleNode( rootNode_, xql );
		return node;
	}

	public Node getRequestedNode(Node node, String xql) throws TransformerException
	{
		node = XPathAPI.selectSingleNode( node, xql );
		return node;
	}

	public NodeList getRequestedNodeList(Node node, String xql) throws TransformerException
	{
		NodeList nodeList = XPathAPI.selectNodeList(node, xql);
		return nodeList;
	}

	public String getAttributeValue(Node node, String attrName) throws TransformerException
	{
		String value = null;
		node = node.getAttributes().getNamedItem(attrName);
		if ( node != null )
		{
		      value = node.getNodeValue();
		}
		return value;
	}
	
	public Node getRootNode(){
		return rootNode_;
	}
}
