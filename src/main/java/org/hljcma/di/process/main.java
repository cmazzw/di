package org.hljcma.di.process;

import com.google.gson.Gson;
import org.hljcma.di.pojo.inputdi;
import org.hljcma.di.pojo.outputdi;
import org.hljcma.di.service.DiRedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.client.*;

@Component
public class main implements CommandLineRunner {
     @Autowired
     DiRedisService redisService;

    @Override
    public  void run(String... args) throws Exception {
        InetAddress inet = InetAddress.getByName("10.96.91.215");
        System.out.println(inet);

        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(inet,11111), "cts", "", "");
        int batchSize = 1000;
        try {
            connector.connect();
            connector.subscribe("CTS_DB.TB_RCV_REPT");
            connector.rollback();
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    printEntry(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } finally {
            connector.disconnect();
        }
    }

    private void printEntry( List<Entry> entrys) {

        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }
            RowChange rowChange = null;
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }
            EventType eventType = rowChange.getEventType();

            for (RowData rowData : rowChange.getRowDatasList()) {
                if(entry.getHeader().getTableName().toString().equals("TB_RCV_REPT")&&eventType == EventType.INSERT)
                {
                    for(Column column : rowData.getAfterColumnsList())
                    {
                        if(column.getName().toString().equals("DATA_SOURCE")&&column.getValue().toString().startsWith("23")) {
                            System.out.println("==============这里是收发详情插入信息，需要导入到es服务器==================");
                            inputdi distring=this.printRowData(rowData);
                            outputdi outstring=this.getoutdi(distring);
                            Gson gson=new Gson();
                            String json=gson.toJson(outstring);
                            redisService.setlist("di",json);
                            //根据rowData生成新数据库行的bean
                            //将这个bean更新到ES大数据环境
                            break;
                        }
                    }
                }
            }
        }
    }

    private inputdi printRowData( RowData rowData) {
        inputdi newdi=new inputdi();
        for(Column column : rowData.getAfterColumnsList())
        {
            if(column.getName().toString().equals("RPT_ID"))
                newdi.setRpt_id(column.getValue());
            if(column.getName().toString().equals("IIIII"))
                newdi.setIiiii(column.getValue());
            if(column.getName().toString().equals("DATA_TYPE_C"))
                newdi.setData_type(column.getValue());
            if(column.getName().toString().equals("REPT_ARRIVE_TIME"))
                newdi.setRpt_arrive_time(column.getValue());
            if(column.getName().toString().equals("FILE_NAME"))
                newdi.setFilename(column.getValue());
            if(column.getName().toString().equals("OBSERVE_TIME"))
                newdi.setObserve_time(column.getValue());
            if(column.getName().toString().equals("REGULAR_DETAIL_HOUR"))
                newdi.setRegular_detail_hour(column.getValue());
            if(column.getName().toString().equals("BBB"))
                newdi.setBbb(column.getValue());
            if(column.getName().toString().equals("REPT_VALIDITY"))
                newdi.setRept_validity(column.getValue());
            if(column.getName().toString().equals("REPT_STATUS"))
                newdi.setRept_status(column.getValue());
            if(column.getName().toString().equals("REPT_COL_TIME"))
                newdi.setRept_col_time(column.getValue());
            if(column.getName().toString().equals("DATA_SOURCE"))
                newdi.setData_source(column.getValue());
            if(column.getName().toString().equals("DATA_SOURCE_NAME"))
                newdi.setData_source_name(column.getValue());
            if(column.getName().toString().equals("PART_DAY"))
                newdi.setPart_day(column.getValue());
            System.out.println(column.getName()+":"+column.getValue());
        }
        return newdi;
    }

    private outputdi getoutdi(inputdi indi){
        String key=indi.getData_type()+"_"+indi.getIiiii();
        outputdi outdi=new outputdi();
        outdi.setData_type_new(redisService.get(key));
        if(outdi.getData_type_new() == null)
           outdi.setData_type_new(indi.getData_type());
        outdi.setRpt_id(indi.getRpt_id());
        outdi.setIiiii(indi.getIiiii());
        outdi.setData_type(indi.getData_type());
        outdi.setRpt_arrive_time(indi.getRpt_arrive_time());
        outdi.setFilename(indi.getFilename());
        outdi.setObserve_time(indi.getObserve_time());
        outdi.setRegular_detail_hour(indi.getRegular_detail_hour());
        outdi.setBbb(indi.getBbb());
        outdi.setRept_validity(indi.getRept_validity());
        outdi.setRept_status(indi.getRept_status());
        outdi.setRept_col_time(indi.getRept_col_time());
        outdi.setData_source(indi.getData_source());
        outdi.setData_source_name(indi.getData_source_name());
        outdi.setPart_day(indi.getPart_day());
        return outdi;
    }
}
