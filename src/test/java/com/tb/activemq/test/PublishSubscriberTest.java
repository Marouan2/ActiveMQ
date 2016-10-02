package com.tb.activemq.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import javax.jms.JMSException;

import org.junit.*;
import org.junit.Test;

import com.tb.activemq.ps.Publisher;
import com.tb.activemq.ps.Subscriber;

public class PublishSubscriberTest {

	 private static Publisher publisherMultipleConsumers, publisherNonDurableSubscriber;
private static Subscriber subscriber1MultipleConsumers, subscriber2MultipleConsumers,
     subscriber1NonDurableSubscriber, subscriber2NonDurableSubscriber;

@BeforeClass
public static void setUpBeforeClass() throws Exception {
 publisherMultipleConsumers = new Publisher();
 publisherMultipleConsumers.create("publisher-multipleconsumers",
         "multipleconsumers.t");

 publisherNonDurableSubscriber = new Publisher();
 publisherNonDurableSubscriber.create("publisher-nondurablesubscriber",
         "nondurablesubscriber.t");

 subscriber1MultipleConsumers = new Subscriber();
 subscriber1MultipleConsumers.create("subscriber1-multipleconsumers",
         "multipleconsumers.t");

 subscriber2MultipleConsumers = new Subscriber();
 subscriber2MultipleConsumers.create("subscriber2-multipleconsumers",
         "multipleconsumers.t");

 subscriber1NonDurableSubscriber = new Subscriber();
 subscriber1NonDurableSubscriber.create(
         "subscriber1-nondurablesubscriber", "nondurablesubscriber.t");

 subscriber2NonDurableSubscriber = new Subscriber();
 subscriber2NonDurableSubscriber.create(
         "subscriber2-nondurablesubscriber", "nondurablesubscriber.t");
}

@AfterClass
public static void tearDownAfterClass() throws Exception {
 publisherMultipleConsumers.closeConnection();
 publisherNonDurableSubscriber.closeConnection();

 subscriber1MultipleConsumers.closeConnection();
 subscriber2MultipleConsumers.closeConnection();
 subscriber1NonDurableSubscriber.closeConnection();
 subscriber2NonDurableSubscriber.closeConnection();
}

@Test
public void testMultipleConsumers() throws IOException {
 try {
     publisherMultipleConsumers.sendFile("log/fileTest1.txt");

     String receiving1 = subscriber1MultipleConsumers.getReceiving(1000,true);
     assertEquals("fileTest1.txt", receiving1);

     String receiving2 = subscriber2MultipleConsumers.getReceiving(1000,true);
     assertEquals("fileTest1.txt", receiving2);

 } catch (JMSException e) {
     fail("a JMS Exception occurred");
 }
}

@Test
public void testNonDurableSubscriber() throws IOException {
 try {
     // nondurable subscriptions, ne reçoit pas les messages envoyés si
     // les subscribers ne sont pas active
     subscriber2NonDurableSubscriber.closeConnection();

     publisherNonDurableSubscriber.sendFile("log/fileTest1.txt");

     // recréation connection pour le nondurable subscription
     subscriber2NonDurableSubscriber.create(
             "subscriber2-nondurablesubscriber",
             "nondurablesubscriber.t");

     publisherNonDurableSubscriber.sendFile("log/fileTest2.txt");

     String receiving1 = subscriber1NonDurableSubscriber.getReceiving(1000,true);
     assertEquals("fileTest1.txt", receiving1);
     String receiving2 = subscriber1NonDurableSubscriber.getReceiving(1000,true);
     assertEquals("fileTest2.txt", receiving2);
     String receiving3 = subscriber2NonDurableSubscriber.getReceiving(1000,true);
     assertEquals("fileTest2.txt", receiving3);
     String receiving4 = subscriber2NonDurableSubscriber.getReceiving(1000,true);
     assertEquals("no receiving", receiving4);

 } catch (JMSException e) {
     fail("a JMS Exception occurred");
 }
}

}
