/*

    Created by Sinatra Gunda
    At 9:12 AM on 8/15/2021

*/
package org.apache.fineract.portfolio.client.helper;

import org.apache.fineract.infrastructure.core.service.Page;
import org.apache.fineract.portfolio.client.data.ClientData;
import org.apache.fineract.portfolio.client.domain.EmailRecipients;
import org.apache.fineract.portfolio.client.domain.EmailRecipientsKey;
import org.apache.fineract.portfolio.client.repo.EmailRecipientsKeyRepository;
import org.apache.fineract.portfolio.client.repo.EmailRecipientsRepository;
import org.apache.fineract.portfolio.client.service.ClientReadPlatformService;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class EmailRecipientsHelper {

    public static Long createMailRecipients(EmailRecipientsKeyRepository emailRecipientsKeyRepository , EmailRecipientsRepository emailRecipientsRepository , EmailRecipientsKey emailRecipientsKey){

        int count = emailRecipientsKey.getEmailRecipientsList().size();
        emailRecipientsKey.setCount(count);
        emailRecipientsKeyRepository.saveAndFlush(emailRecipientsKey);

        /// we need to do a whole lot of logics here
        List<EmailRecipients> emailRecipientsList = emailRecipientsKey.getEmailRecipientsList();

        for(EmailRecipients emailRecipients : emailRecipientsList){
            emailRecipients.setEmailRecipientsKey(emailRecipientsKey);
            emailRecipientsRepository.save(emailRecipients);
        }

        Long id = emailRecipientsKey.getId();
        return id ;

    }

    public static Queue<EmailRecipients> emailRecipients(EmailRecipientsKeyRepository emailRecipientsKeyRepository , EmailRecipientsRepository emailRecipientsRepository , ClientReadPlatformService clientReadPlatformService, Long keyId){

        EmailRecipientsKey emailRecipientsKey = emailRecipientsKeyRepository.findOne(keyId);

        // if true then get office id and fill lists with recipients of clients
        boolean selectAllMode = emailRecipientsKey.getSelectAllMode();
        Queue<EmailRecipients> mailRecipientsQueue = new LinkedList<>();

        if(selectAllMode){

            Long officeId = emailRecipientsKey.getOfficeId();
            Long zero = new Long(0);
            
            if(officeId==null){
                officeId = zero ;
            }
            // we need to implement new nullable stuff here

            // some stupid glitch exists here
            //Optional.ofNullable(officeId)

            boolean status = officeId.equals(zero);
            int cmp = officeId.compareTo(zero);

            Consumer<EmailRecipients> addNewToQueue = (e)-> mailRecipientsQueue.add(e);
                    
            if(officeId.equals(zero)){
                //we taking all clients

                Page<ClientData> clientDataList =  clientReadPlatformService.retrieveAll(null);
                Consumer<ClientData> mailRecipientsConsumer = (clientData) ->{
                    EmailRecipients emailRecipients = createMailRecipientObject(clientData);
                    //Consumer<EmailRecipients> addNew = (e)-> mailRecipientsQueue.put(e);
                    Optional.ofNullable(emailRecipients).ifPresent(addNewToQueue);
                };

                clientDataList.getPageItems().stream().forEach(mailRecipientsConsumer);
            }
            else{

                System.err.println("------- else not select all----------------");
                String lookUpCriteria = String.format("office_id=%d",officeId);

                Collection<ClientData> clientDataList = clientReadPlatformService.retrieveAllForLookup(lookUpCriteria);
                Consumer<ClientData> clientDataConsumer = (clientData)->{

                    EmailRecipients emailRecipients = createMailRecipientObject(clientData);
                    Optional.ofNullable(emailRecipients).ifPresent(addNewToQueue);

                };

                clientDataList.stream().forEach(clientDataConsumer);

            }

            /// update email addressed here son

            Consumer<EmailRecipients> updateEmails = (e)->{
                Long clientId = e.getClientId();
                ClientData clientData = clientReadPlatformService.retrieveOne(clientId);
                String emailCurrent = clientData.getEmailAddress();
                e.setEmailAddress(emailCurrent);
            };

            mailRecipientsQueue.stream().forEach(updateEmails);
            return mailRecipientsQueue ;
        }

        List<EmailRecipients> mailRecipientsList = emailRecipientsRepository.findByEmailRecipientsKeyId(keyId);
        mailRecipientsList.stream().forEach((e)->{
            mailRecipientsQueue.add(e);
        });
        
        return mailRecipientsQueue ;
    }

    public static EmailRecipients createMailRecipientObject(ClientData clientData){

        String name = clientData.displayName();
        String email = clientData.getEmailAddress();
        Long clientId = clientData.getId();

        return new EmailRecipients(name ,email ,true ,clientId);

    }
}