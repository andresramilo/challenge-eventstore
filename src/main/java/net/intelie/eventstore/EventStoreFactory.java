package net.intelie.eventstore;

import net.intelie.challenges.EventStore;


public class EventStoreFactory {
	
	public static EventStore getEventStore() {
		return new EventStoreImpl();
	}
}
