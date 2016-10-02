package com.tb.activemq.ps;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.StreamMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

public class Publisher {
	private static final Logger LOGGER = Logger.getLogger(Publisher.class);
	private String clientId;
	private Connection connection;
	// private Session session;
	private MessageProducer messageProducer;
	private Session session = null;
	private File file;

	public void create(String clientId, String topicName) throws JMSException {
		this.clientId = clientId;
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost:61616");
		// creer une connection
		connection = connectionFactory.createConnection();
		connection.setClientID(clientId);
		//controle des flux
		Properties props = new Properties();
		props.setProperty("prefetchPolicy.producerFlowControl", "false");
		((ActiveMQConnectionFactory) connection).setProperties(props);
		//asynchrone mode
		((ActiveMQConnectionFactory) connection) .setUseAsyncSend (true);
		// creer une session
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// creer le topic
		Destination topic = session.createTopic(topicName);
		// creer un MessageProducer
		messageProducer = session.createProducer(topic);
	}	

	public void sendFile(String fileName) throws JMSException {
		file = new File(fileName);
		//
		InputStream in = null;
		  byte[] buffer = new byte[1024];
		   int c = -1;
		try {
			in = new FileInputStream(file);
			LOGGER.info("*************************Début****************************");
		 while ((c = in.read(buffer)) > 0) {
		// creer un JMS BlobMessage
		StreamMessage streamMessage = session.createStreamMessage();
		streamMessage.setStringProperty("FILE.NAME", file.getName());
		streamMessage.setLongProperty("FILE.SIZE", file.length());
		
		streamMessage.writeBytes(buffer, 0, c);
		// envoyer le message au queue de destination
		messageProducer.send(streamMessage);
		   }
		 LOGGER.info(clientId + ": envoie du fichier=  " + file.getName());
		  } catch (Exception e) {
			  LOGGER.info("erreur envoie du fichier : ", e);
		  }
		
	}
	public void closeConnection() throws JMSException {		 
			    connection.close();		
	}
}
