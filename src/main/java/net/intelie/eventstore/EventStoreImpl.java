package net.intelie.eventstore;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;


/**
 * 
 * @author Andres Romero
 * EventStore implementation that uses a ConcurrentLinkedQueue
 * as event repository. 
 *
 */
public class EventStoreImpl implements EventStore {
	
	/* 
	 * The queue where the events are stored. Uses a linked list as backbone.
	 * This implementation has thread safe operations and 
	 * employs an efficient non-blocking algorithm.
	 */
	private ConcurrentLinkedQueue<Event> eventQueue = new ConcurrentLinkedQueue<>();
	
	public EventStoreImpl() {
		super();
	}

	@Override
	public void insert(Event event) {
		eventQueue.add(event);
	}

	@Override
	public void removeAll(String type) {
		eventQueue.removeIf(event -> type.equals(event.type()));
	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		Predicate<Event> condition = event -> type.equals(event.type()) &&
		    (event.timestamp() >= startTime && 
		     event.timestamp() < endTime);
		Iterator<Event> iterator = eventQueue.stream().filter(condition).iterator();
		return new EventIteratorImpl(iterator);
	}
	
	/**
	 * 
	 * @author Andres Romero
	 * EventStoreIterator implementation that works like an adapter to
	 * java.util.iterator returned by the backed ConcurrentLinkedQueue used as
	 * event repository. The remove method from the original iterator throws a 
	 * UnsupportedMethodException, so we use the source eventQueue to remove
	 * the event directly in order to support this operation. 
	 * 
	 * 
	 *
	 */
	private class EventIteratorImpl implements EventIterator {
		
		private Iterator<Event> iterator;
		private Event current;
		private boolean moveNextWasCalled = false;
		
		private EventIteratorImpl(Iterator<Event> iterator) {
			super();
			this.iterator = iterator;
		}

		@Override
		public void close() throws Exception {
			/* Not implemented since we don't use any resources
			   like files or database connections */
		}

		@Override
		public boolean moveNext() {
			moveNextWasCalled = true;
			if(iterator.hasNext()) {
				current = iterator.next();
				return true;
			}
			current = null;
			return false;
		}

		@Override
		public Event current() {
			isValidState();
			return current;
		}

		@Override
		public void remove() {
			isValidState();
			eventQueue.remove(current);
		}
		
		private void isValidState() {
			if(!moveNextWasCalled || current == null)
				throw new IllegalStateException();
		}

	}
	
}
