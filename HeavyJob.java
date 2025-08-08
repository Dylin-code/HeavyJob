package com.digiplus.social.operation.heavyjob;

import cn.hutool.core.util.IdUtil;
import com.digiplus.social.operation.heavyjob.dao.HeavyJobSubResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 重型任務抽象類別
 * @param <StartParam> 分批起始參數
 * @param <ExeParam> 執行單次分片任務的參數
 * @param <ExeResult> 執行單次分片任務的結果
 */
@RequiredArgsConstructor
@Component
public abstract class HeavyJob<StartParam, ExeParam, ExeResult> {

    private final HeavyJobCreator heavyJobCreator;

    public abstract Class<StartParam> getStartParamClass();
    public abstract Class<ExeParam> getExeParamClass();
    public abstract Class<ExeResult> getExeResultClass();

    /**
     * 啟動重型任務
     * @param batchSize 批量大小 若為0 依照doStart自動計算
     * @param doStartParam 子任務參數
     */
    public final void start(int batchSize, StartParam doStartParam){
        String taskId = IdUtil.objectId();
        start(taskId, batchSize, doStartParam);
    }

    /**
     * 啟動重型任務
     * @param taskId 字定義taskId (需要unique)
     * @param batchSize 批量大小 若為0 依照doStart自動計算
     * @param doStartParam 子任務參數
     */
    public final void start(String taskId, int batchSize, StartParam doStartParam) {
        List<ExeParam> params = doStart(doStartParam);
        String handlerName = this.getClass().getSimpleName();
        heavyJobCreator.createBatch(taskId, batchSize, handlerName, params);
    }

    /**
     * 處理起始參數分批 回傳的List數量與 batchSize一致
     * @return
     */
    public abstract List<ExeParam> doStart(StartParam doStartParam);

    /**
     * 執行單一個分片任務
     * @param param
     * @return
     */
    public abstract ExeResult executeSingleSubJob(ExeParam param);

    /**
     * 結束任務 聚合結果處理(若有)
     * @param endResult
     */
    public abstract void end(List<HeavyJobSubResult> endResult, ExeParam startParam);
}
