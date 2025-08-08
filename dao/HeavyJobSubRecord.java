package com.digiplus.social.operation.heavyjob.dao;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
@TableName("t_heavy_job_sub_record")
public class HeavyJobSubRecord {

    /**
     * 主键ID
     */
    private Long id;

    private String taskId;

    private int batchSize;

    /**
     * 执行状态
     * @see com.digiplus.social.operation.common.TaskStatusEnum
     */
    private String executeStatus;

    private String handlerName;

    private String handlerParams;

    private String resultMongoId;

    @Builder.Default
    private LocalDateTime createdTime = LocalDateTime.now();

    @Builder.Default
    private String delFlag = "0";
}
