package com.tb.activemq.test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import javax.jms.JMSException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tb.activemq.ps.DurableSubscriber;
import com.tb.activemq.ps.Publisher;

public class DurableSubscriberTest {

    private static Publisher publisherDurableSubscriber;
    private static DurableSubscriber subscriber1DurableSubscriber, subscriber2DurableSubscriber;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
      
        publisherDurableSubscriber = new Publisher();
        publisherDurableSubscriber.create("publisher-durablesubscriber",
                "durablesubscriber.t");

        subscriber1DurableSubscriber = new DurableSubscriber();
        subscriber1DurableSubscriber.create("subscriber1-durablesubscriber",
                "durablesubscriber.t", "durablesubscriber1");

        subscriber2DurableSubscriber = new DurableSubscriber();
        subscriber2DurableSubscriber.create("subscriber2-durablesubscriber",
                "durablesubscriber.t", "durablesubscriber2");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        publisherDurableSubscriber.closeConnection();
        
        subscriber1DurableSubscriber.removeDurableSubscriber();
        subscriber2DurableSubscriber.removeDurableSubscriber();

        subscriber1DurableSubscriber.closeConnection();
        subscriber2DurableSubscriber.closeConnection();
    }
   
    @Test
    public void testDurableSubscriber() throws IOException {
        try {
        	// durable subscriptions, reçoit les messages envoyés si
            // les subscribers ne sont pas active
            subscriber2DurableSubscriber.closeConnection();

            publisherDurableSubscriber.sendFile("log/fileTest1.txt");

            subscriber2DurableSubscriber.create(
                    "subscriber2-durablesubscriber", "durablesubscriber.t",
                    "durablesubscriber2");

            publisherDurableSubscriber.sendFile("log/fileTest2.txt");

            String receiving1 = subscriber1DurableSubscriber.getReceiving(1000,true);
            assertEquals("fileTest1.txt", receiving1);
            String receiving2 = subscriber2DurableSubscriber.getReceiving(1000,true);
            assertEquals("fileTest1.txt", receiving2);

            String receiving3 = subscriber1DurableSubscriber.getReceiving(1000,true);
            assertEquals("fileTest2.txt", receiving3);
            String receiving4 = subscriber2DurableSubscriber.getReceiving(1000,true);
            assertEquals("fileTest2.txt", receiving4);

        } catch (JMSException e) {
            fail("a JMS Exception occurred");
        }
    }
}