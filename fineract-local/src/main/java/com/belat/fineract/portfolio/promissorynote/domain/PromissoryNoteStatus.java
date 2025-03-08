package com.belat.fineract.portfolio.promissorynote.domain;

public enum PromissoryNoteStatus {
    ACTIVE(0, "active"),
    CLOSE(1, "close");

    private final Integer value;
    private final String code;

    PromissoryNoteStatus(Integer value, String code) {
        this.value = value;
        this.code = code;
    }
}
