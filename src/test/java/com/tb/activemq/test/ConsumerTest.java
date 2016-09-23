package com.tb.activemq.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tb.activemq.ptp.Consumer;
import com.tb.activemq.ptp.Producer;

public class ConsumerTest {

    private static Producer producerPointToPoint, producerOneConsumer,
            producerNoDependenciesSynchro, producerAcknowledgment;
    
    private static Consumer consumerPointToPoint, consumer1OneConsumer,
            consumer2OneConsumer, consumerNoDependenciesSynchro,
            consumer1Acknowledgment, consumer2Acknowledgment;

    @BeforeClass
    public static void setUpBeforeClass() throws JMSException, NamingException {
        producerPointToPoint = new Producer();
        producerPointToPoint.create("producer-pointtopoint", "pointtopoint.q");

        producerOneConsumer = new Producer();
        producerOneConsumer.create("producer-onlyoneconsumer",
                "onlyoneconsumer.q");

        producerNoDependenciesSynchro = new Producer();
        producerNoDependenciesSynchro.create("producer-notimingdependencies",
                "notimingdependencies.q");

        producerAcknowledgment = new Producer();
        producerAcknowledgment.create("producer-acknowledgeprocessing",
                "acknowledgeprocessing.q");

        consumerPointToPoint = new Consumer();
        consumerPointToPoint.create("consumer-pointtopoint", "pointtopoint.q");

        consumer1OneConsumer = new Consumer();
        consumer1OneConsumer.create("consumer1-onlyoneconsumer",
                "onlyoneconsumer.q");

        consumer2OneConsumer = new Consumer();
        consumer2OneConsumer.create("consumer2-onlyoneconsumer",
                "onlyoneconsumer.q");


        consumer1Acknowledgment = new Consumer();
        consumer1Acknowledgment.create(
                "consumer1-acknowledgeprocessing", "acknowledgeprocessing.q");

        consumer2Acknowledgment = new Consumer();
        consumer2Acknowledgment.create(
                "consumer2-acknowledgeprocessing", "acknowledgeprocessing.q");
    }

    @AfterClass
    public static void tearDownAfterClass() throws JMSException {
        producerPointToPoint.closeConnection();
        producerOneConsumer.closeConnection();
        producerNoDependenciesSynchro.closeConnection();
        producerAcknowledgment.closeConnection();

        consumerPointToPoint.closeConnection();
        consumer1OneConsumer.closeConnection();
        consumer2OneConsumer.closeConnection();
        consumerNoDependenciesSynchro.closeConnection();
        // consumer1Acknowledgment
        consumer2Acknowledgment.closeConnection();
    }

    @Test
    public void testgetReceiving() {
        try {
            producerPointToPoint.sendName("Marouan", "Marouan");

			String receiving = consumerPointToPoint.getReceiving(1000, true);
            assertEquals("Salut Marouan Marouan!", receiving);

        } catch (JMSException e) {
            fail("a JMS Exception occurred");
        }
    }

//Chaque message a un seul consommateur.
    @Test
    public void testOneConsumer() throws InterruptedException {
        try {
            producerOneConsumer.sendName("Nicolas", "Tony");

            String receiving1 = consumer1OneConsumer.getReceiving(1000, true);
            assertEquals("Salut Nicolas Tony!", receiving1);

            Thread.sleep(1000);

            String receiving2 = consumer2OneConsumer.getReceiving(1000, true);
            // chaque message a un seul consommateur
            assertEquals("non recu", receiving2);

        } catch (JMSException e) {
            fail("a JMS Exception occurred");
        }
    }
    
    //Un émetteur et un récepteur d'un message n’ont pas de dépendances de synchronisation.
    @Test
    public void testNoDependenciesSynchro() {
        try {
            producerNoDependenciesSynchro.sendName("Samuel", "Alfred");
            //un récepteur peut récupérer le message même s'il n'est pas en cours d'exécution
            //lorsque le client a envoyé le message
            consumerNoDependenciesSynchro = new Consumer();
            consumerNoDependenciesSynchro.create(
                    "consumer-notimingdependencies", "notimingdependencies.q");

            String receiving = consumerNoDependenciesSynchro.getReceiving(1000,
                    true);
            assertEquals("Salut Samuel Alfred!", receiving);

        } catch (JMSException e) {
            fail("a JMS Exception occurred");
        }
    }

//vérifie que le message n'a pas été supprimé par JMS provider dans le cas où il n'a pas été reconnu par le consommateur.
    @Test
    public void testAcknowledgeProcessing() throws InterruptedException {
        try {
            producerAcknowledgment.sendName("Alexis", "Anna");

            // consommer le message sans acknowledgment
            String receiving1 = consumer1Acknowledgment.getReceiving(1000,
                    false);
            assertEquals("Salut Alexis Anna!", receiving1);

            // close le MessageConsumer, le broker connait qu'il n y a pas
            // d'acknowledgment
            consumer1Acknowledgment.closeConnection();

            String receiving2 = consumer2Acknowledgment.getReceiving(1000,
                    true);
            assertEquals("Salut Alexis Anna!", receiving2);

        } catch (JMSException e) {
            fail("a JMS Exception occurred");
        }
    }
}
