package org.hljcma.di.mapper;

import org.apache.ibatis.annotations.*;
import org.hljcma.di.pojo.datatype;


import java.util.List;


public interface datatypeMapper {


    @Select("select * from FRONT_DATA_TYPE_NEW")
    public List<datatype> getdatatype();
}
