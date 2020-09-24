package com.alibaba.mos.interview;

import com.alibaba.fastjson.JSON;
import com.alibaba.mos.api.ProviderConsumer;
import com.alibaba.mos.api.SkuReadService;
import com.alibaba.mos.data.ChannelInventoryDO;
import com.alibaba.mos.data.ItemDO;
import com.alibaba.mos.data.SkuDO;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 注意： 假设sku数据很多, 无法将sku列表完全加载到内存中
 */
@SpringBootTest
@Slf4j
class Interview2Tests {

    @Autowired
    SkuReadService skuReadService;

    @Autowired
    ProviderConsumer<List<ItemDO>> providerConsumer;
    /** 相同渠道下前五大库存 渠道-库存Top5映射*/
    private Map<String, PriorityQueue<BigDecimal>> map = new HashMap<>();
    /** 所有sku的总价值 */
    private BigDecimal totalPrice = new BigDecimal("0");
    /** 求中间价格的计数器 ,大于等于1元 小于10000元, 数组存放每个价位的sku数量*/
    private int[] count = new int[10000];

    /**
     * 试题1：
     * 注意： 假设sku数据很多, 无法将sku列表完全加载到内存中
     * 在com.alibaba.mos.service.SkuReadServiceImpl中实现com.alibaba.mos.api.SkuReadService#loadSkus(com.alibaba.mos.api.SkuReadService.SkuHandler)
     * 从/resources/data/data.xls读取数据并逐条打印数据
     */
    @Test
    void readDataFromExcelWithHandlerTest() {
        AtomicInteger count = new AtomicInteger();
        skuReadService.loadSkus(skuDO -> {
            log.info("读取SKU信息={}", JSON.toJSONString(skuDO));
            count.incrementAndGet();
            return skuDO;
        });
        Assert.isTrue(count.get() == 10, "未能读取商品列表");
    }

    /**
     * 试题2：
     * 注意： 假设sku数据很多, 无法将sku列表完全加载到内存中
     * 计算以下统计值:
     * 1、获取价格在最中间的任意一个skuId，假设所有sku的价格都是精确到1元且一定小于1万元
     * 2、每个渠道库存量为前五的skuId列表 例如( miao:[1,2,3,4,5],tmall:[3,4,5,6,7],intime:[7,8,4,3,1]
     * 3、所有sku的总价值
     */
    @Test
    void statisticsDataTest() {


        skuReadService.loadSkus(skuDO -> {
            //1. 获取价格在最中间的任意一个skuId
            findMiddlePrice(skuDO);
            //2. 每个渠道库存量为前五的skuId列表
            findTopFiveInventory(skuDO);
            //3. 所有sku的总价值
            calSkuTotalPrice(skuDO);
            return skuDO;
        });
        //1.打印价格在最中间的任意一个skuId
        printMiddlePrice();

        //2.打印每个渠道库存量为前五的skuId列表
        for (Map.Entry<String, PriorityQueue<BigDecimal>> entry : map.entrySet()) {
            List<BigDecimal> price = entry.getValue().stream().sorted().collect(Collectors.toList());
            log.info("每个渠道库存量为前五的skuId列表:{}",
                    entry.getKey() + ":" + price.toString());
        }
        //3.打印所有sku的总价值
        log.info("所有Sku总价值：{}", totalPrice);
    }

    private void printMiddlePrice() {

        long middleCount = sumCount(count) / 2;
        long preCount = 0L;
        //求中间价格
        BigDecimal middlePrice = null;
        for (int i = 1; i < 10000; i++) {
            preCount += count[i];
            if (preCount > middleCount) {
                middlePrice = new BigDecimal(i);
                break;
            }
        }

        //按中间价格搜索skuId
        final BigDecimal middlePri = middlePrice;
        BigDecimal middlePriceAdd1 = middlePrice.add(new BigDecimal("1"));
        skuReadService.loadSkus(skuDO -> {
            if (middlePri.compareTo(skuDO.getPrice()) <= 0 && skuDO.getPrice().compareTo(middlePriceAdd1) < 0) {
                log.info("获取价格在最中间的任意一个skuId:{}", skuDO.getSpuId());
                return skuDO;
            }
            return skuDO;
        });

    }

    private long sumCount(int[] count) {
        long total = 0L;
        for (int i = 1; i < 10000; i++) {
            total += count[i];
        }
        return total;
    }

    private void calSkuTotalPrice(SkuDO skuDO) {
        totalPrice = totalPrice.add(skuDO.getPrice());
    }

    private void findMiddlePrice(SkuDO skuDO) {
        double value = skuDO.getPrice().doubleValue();
        //精确到1元，用100*100长度的数组保存每个价格的skuID产品数量
        if (value < 1 || value >= 10000) {
            return;
        }
        int index = (int) value;
        count[index] += 1;
    }

    private void findTopFiveInventory(SkuDO skuDO) {
        List<ChannelInventoryDO> channelInventorys = skuDO.getInventoryList();
        if (CollectionUtils.isEmpty(channelInventorys)) {
            return;

        }
        for (ChannelInventoryDO ci : channelInventorys) {
            PriorityQueue<BigDecimal> priorityQueue = map.get(ci.getChannelCode());
            if (priorityQueue == null) {
                priorityQueue = new PriorityQueue<>();
                map.put(ci.getChannelCode(), priorityQueue);
            }
            if (priorityQueue.size() < 5) {
                priorityQueue.offer(ci.getInventory());
                continue;
            }
            BigDecimal peek = priorityQueue.peek();
            if (peek.compareTo(ci.getInventory()) < 0) {
                priorityQueue.poll();
                priorityQueue.offer(ci.getInventory());
            }

        }

    }

    /**
     * 试题3:
     * 注意： 假设sku数据很多, 无法将sku列表完全加载到内存中
     * 基于试题1, 在com.alibaba.mos.service.ItemAggregationProviderConsumer中实现一个生产者消费者, 将sku列表聚合为商品, 并通过回调函数返回,
     * 聚合规则为：
     * 对于sku type为原始商品(ORIGIN)的, 按货号(artNo)聚合成ITEM
     * 对于sku type为数字化商品(DIGITAL)的, 按spuId聚合成ITEM
     * 聚合结果需要包含: item的最大价格、最小价格、sku列表及总库存
     */
    @Test
    void aggregationSkusWithConsumerProviderTest() {
        AtomicInteger count = new AtomicInteger();
        providerConsumer.execute(list -> {
            list.forEach(item -> {
                log.info("聚合后ITEM信息={}", JSON.toJSONString(item));
                count.incrementAndGet();
            });
            return list;
        });
        Assert.isTrue(count.get() == 7, "未能聚合商品列表");
    }
}
