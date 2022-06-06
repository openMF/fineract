/*

    Created by Sinatra Gunda
    At 9:05 AM on 12/18/2021

*/
package org.apache.fineract.portfolio.products.domain;
import org.apache.fineract.accounting.common.AccountingRuleType;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.ApiParameterError;
import org.apache.fineract.infrastructure.core.data.DataValidatorBuilder;
import org.apache.fineract.infrastructure.core.domain.AbstractPersistableCustom;
import org.apache.fineract.infrastructure.core.exception.PlatformApiDataValidationException;
import org.apache.fineract.organisation.monetary.domain.MonetaryCurrency;
import org.apache.fineract.organisation.monetary.domain.Money;
import org.apache.fineract.portfolio.charge.domain.Charge;
import org.apache.fineract.portfolio.interestratechart.domain.InterestRateChart;
import org.apache.fineract.portfolio.products.constants.ProductConstants;
import org.apache.fineract.portfolio.products.enumerations.PRODUCT_TYPE;
import org.apache.fineract.portfolio.savings.*;
import org.apache.fineract.portfolio.tax.domain.TaxGroup;

import java.math.BigDecimal;
import java.util.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.joda.time.LocalDate;
import com.google.gson.JsonArray;

@Entity
@Table(name = "m_product")
public class Product extends AbstractPersistableCustom<Long> {

    @Column(name = "product_type", nullable = false)
    protected Integer productType;

    @Column(name = "product_id",nullable=false)
    private Long productId;

    @Column(name = "active" ,nullable =false)
    private Boolean active;

    protected Product(){}


    public Product(Integer productType, Long productId, Boolean active) {
        this.productType = productType;
        this.productId = productId;
        this.active = active;
    }

    public Map<String, Object> update(final JsonCommand command) {

        final Map<String, Object> actualChanges = new LinkedHashMap<>(10);

        final String localeAsInput = command.locale();

        if (command.isChangeInBooleanParameterNamed(ProductConstants.active, this.active)) {
            final boolean newValue = command.booleanObjectValueOfParameterNamed(ProductConstants.active);
            actualChanges.put(ProductConstants.active, newValue);
            this.active = newValue;
        }

        return actualChanges;
    }

    public Integer getProductType() {
        return productType;
    }

    public void setProductType(Integer productType) {
        this.productType = productType;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Boolean isActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }
}