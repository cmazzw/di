package org.hljcma.di.service;

import org.hljcma.di.mapper.datatypeMapper;
import org.hljcma.di.pojo.datatype;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class datatypeService {
    @Autowired
    private datatypeMapper datatypemapper;

    //获取资料类型List
    public List<datatype> getdatatype(){
        return datatypemapper.getdatatype();
    }

    //根据资料类型List和rowdata，重新生成pojo,用于插入数据库
}
