package com.tb.activemq.ptp;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
public class Consumer {
	 private static final Logger LOGGER = Logger.getLogger(Consumer.class);
	    private static String NO_RECEIVING = "non recu";
	    private String clientId;
	    private Connection connection;
	    private Session session;
	    private MessageConsumer messageConsumer;

	    public void create(String clientId, String queueName) throws JMSException {
	        this.clientId = clientId;
	        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost:61616");
	        connection = connectionFactory.createConnection();
	        connection.setClientID(clientId);
	        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
	        Queue queue = session.createQueue(queueName);
	        messageConsumer = session.createConsumer(queue);
	        connection.start();
	    }
	    public void closeConnection() throws JMSException {
	        connection.close();
	    }
	    public String getReceiving(int timeout, boolean acknowledge)
	            throws JMSException {
	        String receiving = NO_RECEIVING;
	        // lire un message 
	        Message message = messageConsumer.receive(timeout);
	        // check si le message a été bien reçu
	        if (message != null) {
	            TextMessage textMessage = (TextMessage) message;
	            // récupérer le contenu du message
	            String text = textMessage.getText();
	            LOGGER.info(clientId + ": reception message avec text=  " + text);
	            if (acknowledge) {
	                // accusé réception du message
	                message.acknowledge();
	                LOGGER.info(clientId + ": message reçu");
	            } else {
	                LOGGER.info(clientId + ": message non reçu");
	            }
	            // crer receiving
	            receiving = "Salut " + text + "!";
	        } else {
	            LOGGER.info(clientId + ": aucun message reçu");
	        }
	        LOGGER.info("Message reçu =  " + receiving);
	        LOGGER.info("*************************Fin****************************");
	        return receiving;
	    }
}
