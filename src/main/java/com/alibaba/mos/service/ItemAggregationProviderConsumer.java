/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2019 All Rights Reserved.
 */
package com.alibaba.mos.service;

import com.alibaba.mos.api.ProviderConsumer;
import com.alibaba.mos.api.SkuReadService;
import com.alibaba.mos.data.ChannelInventoryDO;
import com.alibaba.mos.data.ItemDO;
import com.alibaba.mos.data.SkuDO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.mos.constant.SkuConstant.*;

/**
 * @author superchao
 * @version $Id: ItemAggregationProviderConsumerImpl.java, v 0.1 2019年11月20日 3:06 PM superchao Exp $
 */
@Service
public class ItemAggregationProviderConsumer implements ProviderConsumer<List<ItemDO>> {
    @Autowired
    SkuReadService skuReadService;
    /** 聚合对象映射 */
    Map<String, ItemDO> map = new HashMap<>();

    @Override
    public void execute(ResultHandler<List<ItemDO>> handler) {
        //试题3
        skuReadService.loadSkus(skuDO -> {
            String skuType = skuDO.getSkuType();
            if (ORIGIN.equals(skuType)) {
                //对于sku type为原始商品(ORIGIN)的, 按货号(artNo)聚合成ITEM
                aggregationSkuDO(skuDO,skuType,skuDO.getArtNo());
            } else if (DIGITAL.equals(skuType)) {
                //对于sku type为数字化商品(DIGITAL)的, 按spuId聚合成ITEM
                aggregationSkuDO(skuDO,skuType,skuDO.getSpuId());
            }
            return skuDO;
        });
        List<ItemDO> itemDos = map.values().stream().collect(Collectors.toList());

        handler.handleResult(itemDos);
    }

    private void aggregationSkuDO(SkuDO skuDO,String skuType,String artNo) {
        String key = String.format("%s_%s",skuType,artNo);
        ItemDO itemDO = map.get(key);
        if(itemDO == null){
            itemDO = new ItemDO();
            //创建第一个ItemDO
            newItemDoBySkuDo(skuDO,itemDO);
            map.put(key,itemDO);
        }else{
            //ItemDO已存在，把SkuDO加到ItemDO
            addSku2ItemDo(skuDO,itemDO);
        }
    }

    private void addSku2ItemDo(SkuDO skuDO, ItemDO itemDO) {
        BigDecimal price = skuDO.getPrice();
        if(price != null) {
            if (price.compareTo(itemDO.getMinPrice()) < 0) {
                itemDO.setMinPrice(price);
            }
            if (price.compareTo(itemDO.getMaxPrice()) > 0) {
                itemDO.setMaxPrice(price);
            }
        }
        itemDO.setInventory(sumInventory(skuDO.getInventoryList())
                .add(itemDO.getInventory()));

        itemDO.getSkuIds().add(skuDO.getSpuId());

    }

    private void newItemDoBySkuDo(SkuDO skuDO, ItemDO itemDO) {
        BigDecimal totalInventory = sumInventory(skuDO.getInventoryList());

        itemDO.setArtNo(skuDO.getArtNo());
        itemDO.setInventory(totalInventory);
        itemDO.setMaxPrice(skuDO.getPrice());
        itemDO.setMinPrice(skuDO.getPrice());
        itemDO.setName(skuDO.getName());
        itemDO.setSpuId(skuDO.getSpuId());
        List<String> skuIds = itemDO.getSkuIds();
        if(skuIds == null){
            skuIds = new ArrayList<>();
            itemDO.setSkuIds(skuIds);
        }
        skuIds.add(skuDO.getSpuId());

    }

    private BigDecimal sumInventory(List<ChannelInventoryDO> inventoryList) {
        BigDecimal totalInventory = BigDecimal.ZERO;
        if(!CollectionUtils.isEmpty(inventoryList)){
            for(ChannelInventoryDO channelInventoryDO : inventoryList){
                totalInventory = totalInventory.add(channelInventoryDO.getInventory());
            }
        }
        return totalInventory;

    }
}