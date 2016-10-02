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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableSubscriber {

	private static final Logger LOGGER = LoggerFactory.getLogger(DurableSubscriber.class);

	private static final String NO_RECEIVING = "no receiving";

	private String clientId;
	private Connection connection;
	private Session session;
	private MessageConsumer messageConsumer;
	private String subscriptionName;
	OutputStream outputStream;
	InputStream stream;
	private BufferedOutputStream bos = null;

	public void create(String clientId, String topicName, String subscriptionName) throws JMSException {
		this.clientId = clientId;
		this.subscriptionName = subscriptionName;

		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost:61616");
		// Connection
		connection = connectionFactory.createConnection();
		connection.setClientID(clientId);
		// relivraison des messages qui ont rencontrés des problème au cours
		// d'exécution
		RedeliveryPolicy policy = ((ActiveMQConnectionFactory) connection).getRedeliveryPolicy();
		policy.setInitialRedeliveryDelay(500);
		policy.setBackOffMultiplier(2);
		policy.setUseExponentialBackOff(true);
		policy.setMaximumRedeliveries(2);
		// augmenter l'efficacité de topic subscribers
		Properties props = new Properties();
		props.setProperty("prefetchPolicy.topicPrefetch", "10");
		((ActiveMQConnectionFactory) connectionFactory).setProperties(props);
		// Session
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Topic topic = session.createTopic(topicName);
		messageConsumer = session.createDurableSubscriber(topic, subscriptionName);

		connection.start();
	}

	public void removeDurableSubscriber() throws JMSException {
		messageConsumer.close();
		session.unsubscribe(subscriptionName);
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
			if (message == null) {
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

		// creer receiving
		receiving = fileName;

		return receiving;

	}
}