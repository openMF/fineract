@WorkingCapital
@WorkingCapitalLoanChargesFeature
Feature: WorkingCapitalLoanChargesFeature

  @TestRailId:C80954
  Scenario: Verify Working Capital Charge product - UC1: charge can be created modified and deleted
    When Admin creates working capital loan charge
    When Admin updates working capital loan charge
    When Admin deletes working capital loan charge

  @TestRailId:C80955
  Scenario: Verify Working Capital Charge product - UC2: template API returns filtered options
    Then Admin retrieves the charge template for Working Capital Loan
    Then The charge template chargeTimeTypeOptions contains only Specified due date
    Then The charge template chargeCalculationTypeOptions contains only Flat
    Then The charge template chargePaymentModeOptions contains only Regular

  @TestRailId:C80956
  Scenario: Verify Working Capital Charge product - UC3: template API with Specified due date returns Flat calculation type only
    Then Admin retrieves the charge template for Working Capital Loan with charge time type "SPECIFIED_DUE_DATE"
    Then The charge template chargeCalculationTypeOptions contains only Flat

  @TestRailId:C80957
  Scenario: Verify Working Capital Charge product - UC4: charge can be created as penalty
    When Admin creates working capital loan charge as penalty
    Then Admin retrieves working capital loan charge and verifies it is a penalty
    When Admin deletes working capital loan charge

  @TestRailId:C80958
  Scenario: Verify Working Capital Charge product - UC5: creating charge without payment mode will result defaults to payment mode Regular
    When Admin creates working capital loan charge without payment mode
    Then Admin retrieves working capital loan charge and verifies payment mode is Regular
    When Admin deletes working capital loan charge

  @TestRailId:C80959
  Scenario: Verify Working Capital Charge product - UC6: invalid chargeTimeType Disbursement fails (Negative)
    Then Creating working capital loan charge with "DISBURSEMENT" chargeTimeType and "FLAT" chargeCalculationType results an error with the following data:
      | httpCode | errorMessage                                                                                        |
      | 400      | The parameter `chargeTimeType` must be one of [ 2 ] .                                               |

  @TestRailId:C80960
  Scenario: Verify Working Capital Charge product - UC7: invalid chargeCalculationType Percentage Amount fails (Negative)
    Then Creating working capital loan charge with "SPECIFIED_DUE_DATE" chargeTimeType and "PERCENTAGE_AMOUNT" chargeCalculationType results an error with the following data:
      | httpCode | errorMessage                                                                                                  |
      | 400      | The parameter `chargeCalculationType` must be one of [ 1 ] .                                                  |

  @TestRailId:C80961
  Scenario: Verify Working Capital Charge product - UC8: invalid chargeTimeType Instalment Fee fails (Negative)
    Then Creating working capital loan charge with "INSTALLMENT_FEE" chargeTimeType and "FLAT" chargeCalculationType results an error with the following data:
      | httpCode | errorMessage                                                                                        |
      | 400      | The parameter `chargeTimeType` must be one of [ 2 ] .                                               |
