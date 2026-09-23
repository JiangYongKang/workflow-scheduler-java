package com.github.highcumontoa.workflowschedulerjava.web;

import com.github.highcumontoa.workflowschedulerjava.dto.QuotaSnapshot;
import com.github.highcumontoa.workflowschedulerjava.quota.QuotaManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 配额观察接口。 */
@RestController
@RequestMapping("/api/quota")
public class QuotaController {

    private final QuotaManager quotaManager;

    public QuotaController(QuotaManager quotaManager) {
        this.quotaManager = quotaManager;
    }

    @GetMapping
    public QuotaSnapshot snapshot() {
        return quotaManager.snapshot();
    }
}
