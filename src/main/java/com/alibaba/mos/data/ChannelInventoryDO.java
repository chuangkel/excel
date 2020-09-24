/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2019 All Rights Reserved.
 */
package com.alibaba.mos.data;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 *
 * @author superchao
 * @version $Id: ChannelInventoryDO.java, v 0.1 2019年10月28日 10:42 AM superchao Exp $
 */
@Data
public class ChannelInventoryDO implements Serializable {
    /**
     * 渠道编码, 目前包含: MIAO, TMALL, INTIME 3个渠道
     */
    private String channelCode;
    /**
     * 库存数量, 保留小数点后2位
     */
    private BigDecimal inventory;

    public String getChannelCode() {
        return channelCode;
    }

    public void setChannelCode(String channelCode) {
        this.channelCode = channelCode;
    }

    public BigDecimal getInventory() {
        return inventory;
    }

    public void setInventory(BigDecimal inventory) {
        this.inventory = inventory;
    }
}