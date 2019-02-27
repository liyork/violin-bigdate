package com.wolf.test.kafka.producer;

/**
 * Description:
 * <br/> Created on 03/04/2018 8:45 PM
 *
 * @author 李超
 * @since 1.0.0
 */
public class CustomerObject {

    private int customerId;

    private String customerName;

    public CustomerObject(int customerId, String customerName) {
        this.customerId = customerId;
        this.customerName = customerName;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }
}
