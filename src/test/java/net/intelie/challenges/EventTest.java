package net.intelie.challenges;

import org.junit.Test;

import net.intelie.eventstore.EventStoreFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;

public class EventTest {
    
	private EventStore eventStore = EventStoreFactory.getEventStore();
	
	 
	 /**
	  * The initial Data Set with 30 events of types A,B and C. 
	  */
	@Before
	public void init() {
	    for (int i= 0; i < 10; i++) {
	        eventStore.insert(new Event("A", i));
	   	    eventStore.insert(new Event("B", i));
	    	eventStore.insert(new Event("C", i));
	    }
	 }
	
	 /**
	  * This test reads all events in the Data Set
	  * and verify that 30 events were readen by counting them.  
	  */
	@Test
    public void readAllEvents() throws Exception {
		queryEvents("A", 0, 10, 10);
		queryEvents("B", 0, 10, 10);
		queryEvents("C", 0, 10, 10);
    }
	
	/**
	 * This test inserts an event and then query and read the newly
	 * inserted event asserting the type and timestamp are correct.
	 *  
	 */
	@Test
    public void insertEvent() throws Exception {
		eventStore.insert(new Event("A", 1000));
		EventIterator iterator = eventStore.query("A", 1000, 1001);
		Event event = null;
		if(iterator.moveNext()) {
			event = iterator.current();
			assertEquals(event, new Event("A", 1000));
		}
		assertNotNull(event);
    }
	
	/**
	 * This test removes an event and then query and checking
	 * if event was removed by asserting that iterator returns false.
	 *  
	 */
	@Test
    public void removeEvent() throws Exception {
		EventIterator iterator = eventStore.query("A", 4, 5);
		if(iterator.moveNext())
			iterator.remove();
		iterator = eventStore.query("A", 4, 5);
		assertFalse(iterator.moveNext());
    }
	
	/**
	 * This test removes an event using EventIterator without call moveNext
	 * before. It checks if IllegalStateException was thrown.
	 * 
	 */
	@Test(expected = IllegalStateException.class)
	public void removeEventWithoutCallMoveNext() throws Exception {
		EventIterator iterator = eventStore.query("A", 0, 1);
		iterator.remove();
    }
	
	/**
	 * This test removes an event using EventIterator after a call to moveNext
	 * returned false. It checks if IllegalStateException was thrown.
	 * 
	 */
	@Test(expected = IllegalStateException.class)
	public void removeEventWithMoveNextEqualToFalse() throws Exception {
		EventIterator iterator = eventStore.query("A", 0, 1);
		assertTrue(iterator.moveNext());
		assertFalse(iterator.moveNext());
		iterator.moveNext();
		iterator.remove();
    }
	
	/**
	 * This test gets the current event using EventIterator without call moveNext
	 * before. It checks if IllegalStateException was thrown.
	 * 
	 */
	@Test(expected = IllegalStateException.class)
	public void getCurrentEventWithoutCallMoveNext() throws Exception {
		EventIterator iterator = eventStore.query("A", 0, 1);
		iterator.current();
    }
	
	/**
	 * This test gets the current event using EventIterator after a call to moveNext
	 * returned false. It checks if IllegalStateException was thrown.
	 * 
	 */
	@Test(expected = IllegalStateException.class)
	public void getCurrentEventWithMoveNextEqualToFalse() throws Exception {
		EventIterator iterator = eventStore.query("A", 0, 1);
		assertTrue(iterator.moveNext());
		assertFalse(iterator.moveNext());
		iterator.moveNext();
		iterator.remove();
    }
	
	
	/**
	 * This test checks if all events of a certain type were written
	 * by the initial insertions in the Data Set
	 * 
	 */
	@Test
    public void checkNumberOfEventsOfType() throws Exception {
		EventIterator iterator = eventStore.query("A", 0, 10);
		int count = 0;
		while(iterator.moveNext()) 
		  count++;
		assertEquals(count, 10);
    }
   
	
	/**
	 * This test checks if the query returns the first event inserted
	 * in the Data Set
	 * 
	 */
	@Test
    public void checkFirstElementOfType() throws Exception {
		EventIterator iterator = eventStore.query("A", 0, 1);
		Event event = null;
		if(iterator.moveNext()) {
			event = iterator.current();
			assertEquals(event, new Event("A", 0));
		}
		assertNotNull(event);
    }
	
	/**
	 * This test checks if the query returns the last event inserted
	 * in the Data Set
	 * 
	 */
	@Test
    public void checkLastElementOfType() throws Exception {
		EventIterator iterator = eventStore.query("A", 9, 10);
		Event event = null;
		if(iterator.moveNext()) {
			event = iterator.current();
			assertEquals(event, new Event("A", 9));
		}
		assertNotNull(event);
    }
	
	/**
	 * This test checks if all events of a certain type were removed using
	 * removeAll and querying and asserting this deletion
	 * 
	 */
	@Test
    public void removeAllEventsOfType() throws Exception {
		eventStore.removeAll("C");
		EventIterator iterator = eventStore.query("C", 0, 10);
		assertFalse(iterator.moveNext());
    }
	
	/**
	 * This is a test to check the insertion of events concurrently
	 * with querying and iteration of events. It asserts the newly
	 * events were written.
	 */
	@Test
	public void insertEventsWhileReading() throws InterruptedException {
		Thread iteratorThread = new Thread(() -> {
			EventIterator iterator = eventStore.query("A", 0, 10);
			while(iterator.moveNext()) {
				iterator.current();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		Thread insertThread = new Thread(() -> {
			for (int i= 10; i < 20; i++) {
	            eventStore.insert(new Event("A", i));
	            try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	        }
		});
		 
		insertThread.start();
		iteratorThread.start();
		iteratorThread.join();
	    insertThread.join();
		
		EventIterator iterator = eventStore.query("A", 0, 20);
		int count = 0;
		while(iterator.moveNext())
		    count++;
		assertEquals(count, 20);
		
	}
	
	/**
	 * This is a test to check the removal of events concurrently
	 * with querying and iteration of events. It asserts the events were
	 * removed
	 */
	@Test
	public void removeEventsWhileReading() throws InterruptedException {
		Thread iteratorThread = new Thread(() -> {
			EventIterator iterator = eventStore.query("A", 0, 10);
			while(iterator.moveNext()) {
				iterator.current();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		Thread deletionThread = new Thread(() -> {
			EventIterator iterator = eventStore.query("A", 3, 7);
			while(iterator.moveNext()) {
				iterator.remove();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		iteratorThread.start();
		deletionThread.start();
		iteratorThread.join();
		deletionThread.join();
		
		EventIterator iterator = eventStore.query("A", 0, 10);
		int count = 0;
		while(iterator.moveNext())
		    count++;
		assertEquals(count, 6);
		
	}
	
	/**
	 * This is a test to check the insertion and removal of events concurrently
	 */
	@Test
	public void insertingEventsWhileDeleting() throws InterruptedException {
		Thread insertThread = new Thread(() -> {
			for (int i= 10; i < 20; i++) {
	            eventStore.insert(new Event("A", i));
	            try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	        }
		});
		
		Thread deletionThread = new Thread(() -> {
			EventIterator iterator = eventStore.query("A", 3, 7);
			while(iterator.moveNext()) {
				iterator.remove();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		insertThread.start();
		deletionThread.start();
		insertThread.join();
		deletionThread.join();
		
		EventIterator iterator = eventStore.query("A", 0, 20);
		int count = 0;
		while(iterator.moveNext())
		    count++;
		assertEquals(count, 16);
		
	}
	
	@Test
	public void insertingDeletingAndReading() throws InterruptedException {
		Thread iteratorThread = new Thread(() -> {
			EventIterator iterator = eventStore.query("A", 0, 10);
			while(iterator.moveNext()) {
				iterator.current();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		Thread insertThread = new Thread(() -> {
			for (int i= 10; i < 20; i++) {
	            eventStore.insert(new Event("A", i));
	            try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	        }
		});
		
		Thread deletionThread = new Thread(() -> {
			EventIterator iterator = eventStore.query("A", 3, 7);
			while(iterator.moveNext()) {
				iterator.remove();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		insertThread.start();
		deletionThread.start();
		iteratorThread.start();
		insertThread.join();
		deletionThread.join();
		iteratorThread.join();
		
		EventIterator iterator = eventStore.query("A", 0, 20);
		int count = 0;
		while(iterator.moveNext())
		    count++;
		assertEquals(count, 16);
		
	}
	
	private void queryEvents(String type, long startTime, long endTime, int total) {
		int count = 0;
		EventIterator iterator = eventStore.query(type, startTime, endTime);
		while(iterator.moveNext()) 
		  count++;
		assertEquals(count, total);
	}
    
}