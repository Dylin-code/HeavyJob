package com.digiplus.social.operation.heavyjob;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.digiplus.gateway.common.utils.JsonUtil;
import com.digiplus.social.operation.common.KafkaTopic;
import com.digiplus.social.operation.common.TaskStatusEnum;
import com.digiplus.social.operation.heavyjob.dao.HeavyJobSubRecord;
import com.digiplus.social.operation.heavyjob.dao.HeavyJobSubResult;
import com.digiplus.social.operation.heavyjob.dao.HeavyJobSubRecordRepo;
import com.digiplus.social.operation.utils.MongoLambdaQuery;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 重型任務服務 分散式多實例處理
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class HeavyJobService {

    private final HeavyJobSubRecordRepo heavyJobSubRecordRepo;
    private final HeavyJobFactory heavyJobFactory;
    private final MongoTemplate mongoTemplate;

    /**
     * 處理單個子任務
     */
    @KafkaListener(topics = KafkaTopic.HEAVY_JOB_SUB_TASK_TOPIC, groupId = "heavy-job-group", containerFactory = "retryAndManualAckFactory")
    public <T> void executeSubTask(String messageString, Acknowledgment ack) {
        try {
            HeavyJobSubRecord subTask = JsonUtil.fromStr(messageString, HeavyJobSubRecord.class);
            log.info("executeHeavyJobSubTask handlerName: {}, taskId: {}, params: {}",
                    subTask.getHandlerName(), subTask.getTaskId(), subTask.getHandlerParams());
            // 呼叫執行器執行任務
            @SuppressWarnings("unchecked")
            HeavyJob<?,T,?> heavyJob = heavyJobFactory.getHeavyJob(subTask.getHandlerName());
            Class<T> paramsType = heavyJob.getExeParamClass();
            T handlerParam = JsonUtil.fromStr(subTask.getHandlerParams(), paramsType);
            var result = heavyJob.executeSingleSubJob(handlerParam);
            // 完成任務後更新任務狀態
            subTask.setExecuteStatus(TaskStatusEnum.COMPLETED.getStatus());
            // 若有result需要存 插入mongo
            if (result != null) {
                HeavyJobSubResult heavyJobSubResult = HeavyJobSubResult.builder().result(JsonUtil.toStr(result)).build();
                mongoTemplate.save(heavyJobSubResult);
                subTask.setResultMongoId(heavyJobSubResult.getId());
            }
            heavyJobSubRecordRepo.updateById(subTask);

            // 檢查任務是否全部完成及聚合結果發送
            end(subTask.getTaskId(), heavyJob, handlerParam);
            ack.acknowledge();
        }catch (Exception e){
            log.error("executeHeavyJobSubTaskError" , e);
            throw e;
        }
    }

    /**
     * 檢查任務是否全部完成
     * @param taskId
     * @return
     */
    private boolean isTaskAllDone(String taskId) {
        LambdaQueryWrapper<HeavyJobSubRecord> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(HeavyJobSubRecord::getTaskId, taskId)
                .ne(HeavyJobSubRecord::getExecuteStatus, TaskStatusEnum.COMPLETED.getStatus());
        long count = heavyJobSubRecordRepo.count(queryWrapper);

        // 確保最後一筆查詢正確，可能是因為資料庫還未更新完成，等待800毫秒後再次查詢
        if (count == 1) {
            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            count = heavyJobSubRecordRepo.count(queryWrapper);
        }

        return count == 0;
    }

    /**
     * 任務結束後的處理
     * 1. 聚合資料
     * 2. 發送通知
     * @param taskId
     * @param heavyJob
     */
    private <T> void end(String taskId, HeavyJob heavyJob, T handlerParam) {
        if (!isTaskAllDone(taskId)){
            return;
        }
        List<HeavyJobSubRecord> records = heavyJobSubRecordRepo.lambdaQuery().eq(HeavyJobSubRecord::getTaskId, taskId).list();
        List<String> mongoIds = records.stream().map(HeavyJobSubRecord::getResultMongoId).toList();

        // 若有聚合資料需要發送
        if (!CollectionUtils.isEmpty(mongoIds)){
            var mongoQuery = MongoLambdaQuery.<HeavyJobSubResult>builder().in(HeavyJobSubResult::getId, mongoIds).build();
            List<HeavyJobSubResult> results = mongoTemplate.find(mongoQuery, HeavyJobSubResult.class);
            heavyJob.end(results, handlerParam);
            CompletableFuture.runAsync(() ->
                    results.forEach(item -> {
                item.setDelFlag("1");
                mongoTemplate.save(item);
            }));
        }

        // 結束刪除子任務記錄
        CompletableFuture.runAsync(() ->
                records.forEach(record -> {
            record.setDelFlag("1");
            heavyJobSubRecordRepo.updateById(record);
        }));
    }
}
