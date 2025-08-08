package com.digiplus.social.operation.heavyjob;

import com.digiplus.gateway.common.utils.JsonUtil;
import com.digiplus.gateway.common.utils.KafkaUtil;
import com.digiplus.social.operation.common.KafkaTopic;
import com.digiplus.social.operation.common.TaskStatusEnum;
import com.digiplus.social.operation.heavyjob.dao.HeavyJobSubRecord;
import com.digiplus.social.operation.heavyjob.dao.HeavyJobSubRecordRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Service
public class HeavyJobCreator {

    private final HeavyJobSubRecordRepo heavyJobSubRecordRepo;

    /**
     * 創建批量任務
     */
    public <T> void createBatch(String taskId, int batchSize, String handlerName, List<T> params) {
        if (batchSize <= 0){
            batchSize = params.size();
        }
        List<HeavyJobSubRecord> subTasks = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            HeavyJobSubRecord subTask = HeavyJobSubRecord.builder()
                    .taskId(taskId)
                    .batchSize(batchSize)
                    .handlerName(handlerName)
                    .handlerParams(params != null && i < params.size() ? JsonUtil.toStr(params.get(i)) : "")
                    .executeStatus(TaskStatusEnum.NOT_START.getStatus())
                    .build();
            subTasks.add(subTask);
        }
        heavyJobSubRecordRepo.saveBatch(subTasks);
        // 將任務推送到Kafka 多實例消費
        subTasks.forEach(subTask -> KafkaUtil.pushMsg(KafkaTopic.HEAVY_JOB_SUB_TASK_TOPIC, subTask));
        log.info("HeavyJobCreated handlerName: {}, taskId: {}, batchSize: {}, params: {}",
                handlerName, taskId, batchSize, params);
    }

    /**
     * 檢查意外遺漏的子任務 重新發送mq
     * 範圍10~50分鐘內 避免錯誤資料等積壓
     */
    public void checkMissSubTask(){
        var missTasks = heavyJobSubRecordRepo.lambdaQuery()
                .eq(HeavyJobSubRecord::getDelFlag, "0")
                .eq(HeavyJobSubRecord::getExecuteStatus, TaskStatusEnum.NOT_START.getStatus())
                .lt(HeavyJobSubRecord::getCreatedTime, LocalDateTime.now().minusMinutes(10))
                .gt(HeavyJobSubRecord::getCreatedTime, LocalDateTime.now().minusMinutes(50))
                .list();
        if (!CollectionUtils.isEmpty(missTasks)){
            log.info("HeavyJobCheckMissSubTaskFound {} missed sub tasks, will retry", missTasks.size());
            missTasks.forEach(subTask -> KafkaUtil.pushMsg(KafkaTopic.HEAVY_JOB_SUB_TASK_TOPIC, subTask));
        }
    }
}
