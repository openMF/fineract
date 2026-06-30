/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.integrationtests.common;

import com.google.gson.Gson;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.builder.ResponseSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.restassured.specification.ResponseSpecification;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.fineract.integrationtests.common.accounting.Account;
import org.apache.fineract.integrationtests.common.accounting.AccountHelper;
import org.apache.fineract.integrationtests.common.loans.LoanProductTestBuilder;
import org.apache.fineract.integrationtests.common.loans.LoanTransactionHelper;
import org.apache.fineract.integrationtests.common.provisioning.ProvisioningHelper;
import org.apache.fineract.integrationtests.common.provisioning.ProvisioningTransactionHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers {@code DELETE /provisioningcategory/{id}} — see FINERACT-2653. The endpoint was broken in several ways
 * (create-validation run on a body-less DELETE, a transient entity with a null id, and an in-use check querying a
 * non-existent table); these tests exercise both the success and the in-use-rejected paths end to end.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ProvisioningCategoryDeleteIntegrationTest {

    private RequestSpecification requestSpec;
    private ResponseSpecification responseSpec;
    private AccountHelper accountHelper;
    private LoanTransactionHelper loanTransactionHelper;

    @BeforeEach
    public void setup() {
        Utils.initializeRESTAssured();
        this.requestSpec = new RequestSpecBuilder().setContentType(ContentType.JSON).build();
        this.requestSpec.header("Authorization", "Basic " + Utils.loginIntoServerAndGetBase64EncodedAuthenticationKey());
        this.requestSpec.header("Fineract-Platform-TenantId", "default");
        this.responseSpec = new ResponseSpecBuilder().expectStatusCode(200).build();
        this.accountHelper = new AccountHelper(this.requestSpec, this.responseSpec);
        this.loanTransactionHelper = new LoanTransactionHelper(this.requestSpec, this.responseSpec);
    }

    @Test
    public void testDeleteUnusedProvisioningCategorySucceeds() {
        final ProvisioningTransactionHelper transactionHelper = new ProvisioningTransactionHelper(requestSpec, responseSpec);

        final String categoryName = Utils.randomStringGenerator("PROV_CAT_", 6);
        final Integer categoryId = transactionHelper.createProvisioningCategory("{\"categoryname\":\"" + categoryName + "\"}");
        Assertions.assertNotNull(categoryId);

        // An unused category must delete cleanly. Regression guard: the in-use check previously queried a
        // non-existent table (m_loanproduct_provisioning_details) and failed on every database.
        final Integer deletedId = transactionHelper.deleteProvisioningCategory(categoryId);
        Assertions.assertEquals(categoryId, deletedId);

        final ArrayList categories = transactionHelper.retrieveAllProvisioningCategories();
        for (Object category : categories) {
            Assertions.assertNotEquals(categoryId, ((Map) category).get("id"));
        }
    }

    @Test
    public void testDeleteProvisioningCategoryInUseIsRejected() {
        final ProvisioningTransactionHelper transactionHelper = new ProvisioningTransactionHelper(requestSpec, responseSpec);

        // A brand-new category, referenced from a criteria below — so the test never touches the seed categories.
        final String categoryName = Utils.randomStringGenerator("PROV_CAT_", 6);
        final Integer categoryId = transactionHelper.createProvisioningCategory("{\"categoryname\":\"" + categoryName + "\"}");
        Assertions.assertNotNull(categoryId);

        final Integer loanProductId = createLoanProduct();
        Assertions.assertNotNull(loanProductId);
        final ArrayList<Integer> loanProducts = new ArrayList<>();
        loanProducts.add(loanProductId);
        final Account liability = accountHelper.createLiabilityAccount();
        final Account expense = accountHelper.createExpenseAccount();

        // Reference only the fresh category under test, so the criteria is valid regardless of any pre-existing
        // provisioning data on the tenant (and so the test stays isolated from the seed categories).
        final ArrayList allCategories = transactionHelper.retrieveAllProvisioningCategories();
        final ArrayList categoriesUnderTest = new ArrayList();
        for (Object category : allCategories) {
            if (categoryId.equals(((Map) category).get("id"))) {
                categoriesUnderTest.add(category);
            }
        }
        final Map requestCriteria = ProvisioningHelper.createProvisioingCriteriaJson(loanProducts, categoriesUnderTest, liability, expense);
        final Integer criteriaId = transactionHelper.createProvisioningCriteria(new Gson().toJson(requestCriteria));
        Assertions.assertNotNull(criteriaId);

        // Delete must be rejected with the domain-rule violation (HTTP 403), NOT a SQL/table error.
        final ResponseSpecification errorSpec = new ResponseSpecBuilder().expectStatusCode(403).build();
        final ArrayList<HashMap> error = (ArrayList<HashMap>) transactionHelper.deleteProvisioningCategoryExpectingError(errorSpec,
                categoryId);
        Assertions.assertEquals("error.msg.provisioningcategory.cannot.be.deleted.it.is.already.used.in.loanproduct",
                error.get(0).get(CommonConstants.RESPONSE_ERROR_MESSAGE_CODE));

        // Once the referencing criteria is removed, the category can be deleted.
        transactionHelper.deleteProvisioningCriteria(criteriaId);
        final Integer deletedId = transactionHelper.deleteProvisioningCategory(categoryId);
        Assertions.assertEquals(categoryId, deletedId);
    }

    private Integer createLoanProduct() {
        final String loanProductJSON = new LoanProductTestBuilder() //
                .withPrincipal("100000.00") //
                .withNumberOfRepayments("4") //
                .withRepaymentAfterEvery("1") //
                .withRepaymentTypeAsMonth() //
                .withinterestRatePerPeriod("1") //
                .withInterestRateFrequencyTypeAsMonths() //
                .withAmortizationTypeAsEqualInstallments() //
                .withInterestTypeAsDecliningBalance() //
                .withTranches(false) //
                .withAccountingRuleAsNone() //
                .build(null);
        return this.loanTransactionHelper.getLoanProductId(loanProductJSON);
    }
}
