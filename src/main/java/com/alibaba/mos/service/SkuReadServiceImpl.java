/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2019 All Rights Reserved.
 */
package com.alibaba.mos.service;

import com.alibaba.mos.api.SkuReadService;
import com.alibaba.mos.util.SkuDoEventListener;
import org.springframework.stereotype.Service;

/**
 * TODO: 实现
 *
 * @author superchao
 * @version $Id: SkuReadServiceImpl.java, v 0.1 2019年10月28日 10:49 AM superchao Exp $
 */
@Service
public class SkuReadServiceImpl implements SkuReadService {


    private static final Object VALUE_TYPE = 0;

    /**
     * 这里假设excel数据量很大无法一次性加载到内存中
     *
     * @param handler
     */
    @Override
    public void loadSkus(SkuHandler handler) {
         new SkuDoEventListener(handler);
    }
}