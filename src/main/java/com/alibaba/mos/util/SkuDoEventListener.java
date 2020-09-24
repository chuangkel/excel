package com.alibaba.mos.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.mos.api.SkuReadService;
import com.alibaba.mos.data.ChannelInventoryDO;
import com.alibaba.mos.data.SkuDO;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.record.*;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;

/**
 * 通过事件模式读取Xls,需要继承HSSFListener
 * 存在不足：{@link com.alibaba.mos.util.SkuDoEventListener#stringRecordList } 字段存储了所有的字符类型的单元格值，
 * 还是存在大对象内存溢出的可能性，2种改进：1.后面考虑用堆外内存实现（暂未实现） 2.或不缓存该对象，直接根据RowRecord去内存去（暂不知道怎么取）
 * @author lpc
 * @version $Id: A.java, v 0.1 2020年09月10日 13:56:46 lpc Exp $
 */
public class SkuDoEventListener implements HSSFListener {
    /** 跳过表头，从第一行数据开始 */
    private int rowId = 1;
    private SSTRecord stringRecordList;
    private SkuReadService.SkuHandler handler;

    public SkuDoEventListener(SkuReadService.SkuHandler handler) {
        this.handler = handler;
        try {
            // 文件inputStream
            FileInputStream is = new FileInputStream("src\\main\\resources\\data\\skus.xls");
            POIFSFileSystem poifs = new POIFSFileSystem(is);
            InputStream din = poifs.createDocumentInputStream("Workbook");
            HSSFRequest req = new HSSFRequest();
            // 为HSSFRequest增加listener
            req.addListenerForAllRecords(this);
            HSSFEventFactory factory = new HSSFEventFactory();
            // 处理inputstream
            factory.processEvents(req, din);
            // 关闭inputstream
            is.close();
            din.close();
        } catch (IOException e) {
        }

    }

    /**
     * 实现接口方法用于处理每一条记录，包括workbook/row/cell
     */
    @Override
    public void processRecord(Record record) {
        switch (record.getSid()) {
            //处理数字单元格
            case NumberRecord.sid:
                NumberRecord numberRecord = (NumberRecord) record;
                if (rowId == numberRecord.getRow()) {

                    int colId = numberRecord.getColumn();
                    double value = numberRecord.getValue();
                    addDoubleValue(colId, value);
                }
                break;
            // 包含一行中所有文本单元格
            case SSTRecord.sid:
                stringRecordList = (SSTRecord) record;
                break;

            //处理文本单元格
            case LabelSSTRecord.sid:
                LabelSSTRecord labelSstRecord = (LabelSSTRecord) record;
                if (rowId == labelSstRecord.getRow()) {
                    int colId = labelSstRecord.getColumn();
                    String value = stringRecordList.getString(labelSstRecord.getSSTIndex()).toString();
                    addValueString(colId, value);
                }
                break;

            default:
        }
    }

    SkuDO skuDO = new SkuDO();

    private void addDoubleValue(int colId, double value) {
        switch (colId) {
            case 0:
                skuDO.setId(String.valueOf((int) value));
                break;
            case 3:
                skuDO.setSpuId(String.valueOf((int) value));
                break;
            case 5:
                skuDO.setPrice(new BigDecimal(value));
                break;
            default:
        }
    }

    private void addValueString(int colId, String value) {

        switch (colId) {
            case 1:
                skuDO.setName(value);
                break;
            case 2:
                skuDO.setArtNo(value);
                break;
            case 4:
                skuDO.setSkuType(value);
                break;
            case 6:
                //每一行最后一列 处理skuDo,清除skuDo,进入下一行
                skuDO.setInventoryList(JSONArray.parseArray(value, ChannelInventoryDO.class));
                handler.handleSku(skuDO);
                clearSkuDo();
                rowId++;
                break;
            default:

        }
    }

    private void clearSkuDo() {
        skuDO.setId(null);
        skuDO.setName(null);
        skuDO.setSpuId(null);
        skuDO.setSkuType(null);
        skuDO.setInventoryList(null);
        skuDO.setPrice(null);
        skuDO.setArtNo(null);
    }
}