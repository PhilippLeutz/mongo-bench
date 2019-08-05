/*                                                                               
 * Copyright (c) 2017, IBM All rights reserved.                                  
 *                                                                               
 * Licensed under the Apache License, Version 2.0 (the "License"); you           
 * may not use this file except in compliance with the License. You              
 * may obtain a copy of the License at                                           
 *                                                                               
 * http://www.apache.org/licenses/LICENSE-2.0                                    
 *                                                                               
 * Unless required by applicable law or agreed to in writing, software           
 * distributed under the License is distributed on an "AS IS" BASIS,             
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or               
 * implied. See the License for the specific language governing                  
 * permissions and limitations under the License. See accompanying               
 * LICENSE file.                                                                 
 */

package com.ibm.mongo;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClientURI;

public final class MongoURI {
	public static List<String> host = new ArrayList<String>();
	public static String username;
	public static String password;
	public static String replica;
	public static boolean isSSLEnabled;

	private MongoURI() {
		username = "xxxx";
		password = "xxxx";
		replica = "xxxx";
		isSSLEnabled = false;
	}

	public static void parseURI(String uri) {
		MongoClientURI clientURI = new MongoClientURI(uri);
		host = clientURI.getHosts();
		try {
			username = clientURI.getUsername();
		} catch (Exception e) {
			username = "";
		}
		try {
			password = new String(clientURI.getPassword());

		} catch (Exception e) {
			password = "";
		}
		
		replica = clientURI.getOptions().getRequiredReplicaSetName();
		isSSLEnabled = uri.toLowerCase().contains("ssl");
	}

	public static String createURI(List<String> host, String username, 
			String password, String replica, boolean sslEnabled) {
		String uri = "mongodb://";
		if (!"".equals(username) && !"".equals(password)) {
			uri = uri + username + ":" + password + "@";
		} 

		uri = (uri + host.toString().replace(", ", ",")
				.replace("[", "")
				.replace("]", "")
				+ "/");

		if (!"".equals(replica)) {
			uri = uri + "?replicaSet=" + replica;
		}

		if (sslEnabled == true) {
			if (!"".equals(replica)) {
				uri = uri + "&ssl=true";
			} else {
				uri = uri + "?ssl=true";
			}
		}

		return uri;
	} 
}
