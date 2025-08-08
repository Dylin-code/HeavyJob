package com.digiplus.social.operation.heavyjob.dao;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
@Document(collection = "heavy_job_sub_result")
public class HeavyJobSubResult {

    @Id
    private String id;

    /**
     * 批次任務任意內容
     */
    private String result;

    @Builder.Default
    private String delFlag = "0";
}
