package com.tb.activemq.ps;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.BlobMessage;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.log4j.Logger;

import com.tb.activemq.ptp.Consumer;

public class Subscriber {
	private static final Logger LOGGER = Logger.getLogger(Subscriber.class);
	private static String NO_RECEIVING = "non recu";
	private String clientId;
	private Connection connection;
	private Session session;
	private MessageConsumer messageConsumer;
	OutputStream outputStream;
	InputStream stream;
	private BufferedOutputStream bos = null;

	public void create(String clientId, String topicName) throws JMSException {
		this.clientId = clientId;
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost:61616");
		connection = connectionFactory.createConnection();
		connection.setClientID(clientId);
		//relivraison des messages qui ont rencontrés des problème au cours d'exécution
		RedeliveryPolicy policy = ((ActiveMQConnectionFactory) connection).getRedeliveryPolicy();
		policy.setInitialRedeliveryDelay(500);
		policy.setBackOffMultiplier(2);
		policy.setUseExponentialBackOff(true);
		policy.setMaximumRedeliveries(2);
		//augmenter l'efficacité de topic subscribers
		Properties props = new Properties();
		 props.setProperty("prefetchPolicy.topicPrefetch", "10");
		 ((ActiveMQConnectionFactory) connectionFactory).setProperties(props);
		session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		// creer le topic
		Topic topic = session.createTopic(topicName);
		messageConsumer = session.createConsumer(topic);
		connection.start();
	}

	public void closeConnection() throws JMSException {
		connection.close();
	}

	public String getReceiving(int timeout, boolean acknowledge) throws JMSException, IOException {
		String receiving = NO_RECEIVING;
		String fileName = null;
		Message message;
		// lire un message
		 byte[] buffer = new byte[2048];
		 
	while (true) {
		message = messageConsumer.receive(timeout);
		if(message==null){
			break;
		}
		// check si le message a été bien reçu
		if (message instanceof StreamMessage) {
			StreamMessage blobMessage = (StreamMessage) message;
			int c = blobMessage.readBytes(buffer);
			// récupérer le contenu du message
			fileName = blobMessage.getStringProperty("FILE.NAME");
			File file = new File(fileName);			  
			outputStream = new FileOutputStream(file);
			bos = new BufferedOutputStream(outputStream);
			  String tempStr = new String(buffer, 0, c);
			  LOGGER.info(clientId + ": reception du fichier =  " + fileName);
			     LOGGER.info("Contenu du fichier: " + tempStr);
			     bos.write((tempStr).getBytes());
			
			if (acknowledge) {
				// accusé réception du message
				message.acknowledge();
				LOGGER.info(clientId + ": Accusé de reception fichier bien reçu");
			} else {
				LOGGER.info(clientId + ": Aucun fichier reçu");
			}	
			LOGGER.info("*****************************************************");
		}		
	}
	 LOGGER.info("*************************Fin****************************");		
			receiving = fileName;		
		return receiving;
		   
	}

}
