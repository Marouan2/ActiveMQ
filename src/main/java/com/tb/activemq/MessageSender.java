package com.tb.activemq;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.log4j.Logger;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

/**Classe de production des messages
 * @author Marouan
 *
 */
public class MessageSender {
	private JmsTemplate jmsTemplate;
	private Destination destination;
	
	final static Logger LOGGER = Logger.getLogger(MessageSender.class);
	
	public JmsTemplate getJmsTemplate() {
		return jmsTemplate;
	}

	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}
	
	public Destination getDestination() {
		return destination;
	}

	public void setDestination(Destination destination) {
		this.destination = destination;
	}

	public void sendMessage(final String msg) {
		LOGGER.info("***********Début d'envoi du message***********");
		
		LOGGER.info("       Le producteur envoi : " + msg);		
		jmsTemplate.send(destination, new MessageCreator() {
			public Message createMessage(Session session) throws JMSException {
				return session.createTextMessage(msg);
			}});
		LOGGER.info("***********Message envoyé***********");
	}
}
