package com.atguigu.bean;

/**
 * @author smh
 * @create 2021-08-02 11:47
 */
import lombok.Data;

import java.math.BigDecimal;

@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}