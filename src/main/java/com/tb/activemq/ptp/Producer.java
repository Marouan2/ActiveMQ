package com.tb.activemq.ptp;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

public class Producer {
	private static final Logger LOGGER = Logger
            .getLogger(Producer.class);
    private String clientId;
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;
    public void create(String clientId, String queueName) throws JMSException {
        this.clientId = clientId;
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost:61616");
        // creer une connection
        connection = connectionFactory.createConnection();
        connection.setClientID(clientId);
        // creer une session
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // creer une Queue 
        Destination queue = session.createQueue(queueName);
       // creer un MessageProducer 
        messageProducer = session.createProducer(queue);
    }
    public void closeConnection() throws JMSException {
        connection.close();
    }
    public void sendName(String firstName, String lastName) throws JMSException {
        String text = firstName + " " + lastName;
        // creer un JMS TextMessage
        TextMessage textMessage = session.createTextMessage(text);
        // envoyer le message au queue de destination
        messageProducer.send(textMessage);
        LOGGER.info("*************************Début****************************");
        LOGGER.info(clientId + ": envoie un message avec text=  "+ text);
    }
}
