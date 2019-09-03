package com.ibm.mongo.indicies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

public class IndiciesIteratorTest {
	static int[] histogram = new int[50];

	@Test
	public void basicTest() throws InterruptedException {
		ArrayList<Integer> testIndex = new ArrayList<>();
		for(int i = 0; i < 50; i ++){
			testIndex.add(new Integer(i));
		}

		IterationFromList indiciesIterator = new IterationFromList(testIndex);
		ArrayList<Thread> threadList = new ArrayList<>();
		for(int numberOfThreads = 0; numberOfThreads < 5; numberOfThreads++){
			Thread currentThread = new Thread(new IncrementThread(indiciesIterator));
			currentThread.run();
			threadList.add(currentThread);
		}
		
		for(Thread currentThread : threadList){
			currentThread.join();
		}
		System.out.println(Arrays.toString(histogram));
		
		for(int i = 0; i < histogram.length; i++){
			assert(histogram[i] == 10);
		}
	}
	
	@Test
	public void indexReadTest() throws InterruptedException, IOException {
		IterationFromList indiciesIterator = new IterationFromList("ressources/limits");
		System.out.println(indiciesIterator.getIndexSize());

	}
	
	
	class IncrementThread implements Runnable{
		IterationFromList iterator;

		public IncrementThread(IterationFromList iterator) {
			this.iterator = iterator;
		}
		@Override
		public void run() {
			for(int x = 0; x < 100; x++){
				IndiciesIteratorTest.histogram[iterator.getNextIndex()]++;
			}	
		}
	}

}

