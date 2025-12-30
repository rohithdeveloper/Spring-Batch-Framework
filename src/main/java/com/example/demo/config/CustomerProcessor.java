package com.example.demo.config;


import org.springframework.batch.item.ItemProcessor;

import com.example.demo.model.Customer;

public class CustomerProcessor implements ItemProcessor<Customer,Customer> {

    @Override
    public Customer process(Customer customer) throws Exception {
       return customer;
    }
}