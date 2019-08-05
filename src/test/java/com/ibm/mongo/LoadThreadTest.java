package com.ibm.mongo;

import static org.junit.Assert.*;

import org.bson.Document;
import org.junit.Test;

import com.ibm.mongo.LoadThread.DocType;

public class LoadThreadTest {

	@Test
	public void testRandomDocuments() {
		LoadThread thread = new LoadThread(0, null, 0, 20, 20, 0, 0, DocType.random, "",false);
		Document[] documents = thread.createDocuments(4, 4, DocType.random);
		for (Document currentDocument : documents) {
			System.out.println(currentDocument);
		}
	}

	/**@Test
	public void testJsonDocuments() {
		LoadThread thread = new LoadThread(0, null, 0, 20, 20, 0, 0, DocType.json, "/mnt/c/Users/PhilippLeutz/Documents/repositories/mongo-bench/ressources/citylots.json/");
		Document[] documents = thread.createDocuments(4, 4, DocType.json);
		for (Document currentDocument : documents) {
			System.out.println(currentDocument);
		}
	}
**/
}

