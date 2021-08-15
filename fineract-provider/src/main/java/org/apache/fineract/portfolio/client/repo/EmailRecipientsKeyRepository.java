/*

    Created by Sinatra Gunda
    At 8:55 AM on 8/15/2021

*/
package org.apache.fineract.portfolio.client.repo;

import org.apache.fineract.portfolio.client.domain.EmailRecipientsKey;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;


public interface EmailRecipientsKeyRepository extends JpaRepository<EmailRecipientsKey ,Long>  ,JpaSpecificationExecutor<EmailRecipientsKey>{

    List<EmailRecipientsKey> findAll();
    EmailRecipientsKey findOne(Long id);

}
