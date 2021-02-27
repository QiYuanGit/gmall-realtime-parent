package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Author: Felix
 * Date: 2021/2/27
 * Desc: 访客统计接口
 */
public interface VisitorStatsMapper {
    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct,sum(sv_ct) sv_ct, sum(uj_ct) uj_ct,sum(dur_sum) dur_sum " +
        " from visitor_stats_0820 where toYYYYMMDD(stt)=#{date} group by is_new")
    List<VisitorStats> selectVisitorStatsByNewFlag(int date);
}
