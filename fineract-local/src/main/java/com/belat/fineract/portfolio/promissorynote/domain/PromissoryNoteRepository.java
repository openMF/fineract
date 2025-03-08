package com.belat.fineract.portfolio.promissorynote.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface PromissoryNoteRepository extends JpaRepository<PromissoryNote, Long>, JpaSpecificationExecutor<PromissoryNote> {

    @Query(value = "SELECT p.* FROM e_promissory_note p WHERE p.promissory_note_number = ?1", nativeQuery = true)
    PromissoryNote retrieveOneByPromissoryNoteNumber(String promissoryNumber);
}
