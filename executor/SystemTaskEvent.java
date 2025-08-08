package com.digiplus.social.operation.heavyjob.executor;

import com.digiplus.social.operation.common.TaskStatusEnum;
import com.digiplus.social.operation.facade.resp.PageUsers;
import com.digiplus.social.operation.heavyjob.HeavyJob;
import com.digiplus.social.operation.heavyjob.HeavyJobCreator;
import com.digiplus.social.operation.repo.SystemTaskLogRepo;
import com.digiplus.social.operation.repo.SystemTaskRepo;
import com.digiplus.social.operation.heavyjob.dao.HeavyJobSubResult;
import com.digiplus.social.operation.repo.dao.entity.SystemTaskLog;
import com.digiplus.social.operation.service.FeignService;
import com.digiplus.social.operation.service.MessageTemplateService;
import jakarta.annotation.Resource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 事件類型系統任務
 */
@Slf4j
@Service
public class SystemTaskEvent
        extends HeavyJob<SystemTaskEvent.DoStartParam, SystemTaskEvent.ExeParam, SystemTaskEvent.ExeResult> {

    @Resource
    private SystemTaskRepo systemTaskRepo;
    @Resource
    private FeignService feignService;
    @Resource
    private MessageTemplateService messageTemplateService;
    @Resource
    private SystemTaskLogRepo systemTaskLogRepo;

    public SystemTaskEvent(HeavyJobCreator heavyJobCreator) {
        super(heavyJobCreator);
    }

    @Data
    public static class DoStartParam {
        private long current = 1L;
        private long size = 1000L;
        private long total;
        private long systemTaskId;
        private LocalDateTime startTime = LocalDateTime.now();
    }
    @Data
    public static class ExeParam {
        private long current = 1L;
        private long size = 1000L;
        private long systemTaskId;
        private LocalDateTime startTime;
    }
    @Data
    public static class ExeResult {

    }


    @Override
    public Class<DoStartParam> getStartParamClass() {
        return DoStartParam.class;
    }

    @Override
    public Class<ExeParam> getExeParamClass() {
        return ExeParam.class;
    }

    @Override
    public Class<ExeResult> getExeResultClass() {
        return ExeResult.class;
    }

    @Override
    public List<ExeParam> doStart(DoStartParam doStartParam) {
        List<ExeParam> params = new ArrayList<>();
        //依據數據total總數計算分片
        long total = doStartParam.getTotal();
        long size = doStartParam.getSize();

        long pageCount = (total + size - 1) / size; // ceiling division

        for (long i = 2; i <= pageCount; i++) {
            ExeParam param = new ExeParam();
            param.setCurrent(i);
            param.setSize(size);
            param.setSystemTaskId(doStartParam.getSystemTaskId());
            param.setStartTime(doStartParam.getStartTime());
            params.add(param);
        }
        return params;
    }

    @Override
    public ExeResult executeSingleSubJob(ExeParam param) {
        systemTaskRepo.getOptById(param.getSystemTaskId()).ifPresent(systemTask -> {
            PageUsers pageUsers = feignService.getSystemTaskEventUsers(systemTask, param.getCurrent(), param.getSize());
            messageTemplateService.sendTemplateMessage(systemTask, pageUsers.getLoginNames());
        });
        return null;
    }

    @Override
    public void end(List<HeavyJobSubResult> endResult, ExeParam lastTaskParam) {
        systemTaskRepo.getOptById(lastTaskParam.getSystemTaskId()).ifPresent(systemTask -> {
            SystemTaskLog systemTaskLog = SystemTaskLog.builder()
                    .taskId(systemTask.getId())
                    .state(TaskStatusEnum.COMPLETED.getStatus())
                    .execTime(lastTaskParam.getStartTime())
                    .build();
            systemTaskLogRepo.save(systemTaskLog);
            systemTask.setLastExecTime(LocalDateTime.now());
            systemTaskRepo.save(systemTask);
        });
    }
}
