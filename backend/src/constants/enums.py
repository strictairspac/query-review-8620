ROLES=('admin','operator','viewer')
# 任务未终态：queued 排队；executing 正在选库/等库/执行。
# “等库”是执行过程中的事件阶段而非独立状态，页面按事件时间线展示。
# running 为旧版本遗留值，只用于恢复历史未完成任务
PENDING_STATUSES=('queued','executing','running')
# worker 只能抢占 queued；executing 视为已有 worker 正在执行，由恢复流程先改回 queued
CLAIMABLE_STATUSES=('queued',)
STATUSES=('queued','executing','passed','different','failed')
# 主库全局唯一的回退名额（会话级 advisory lock）
FALLBACK_SLOT_LOCK_KEY=4101001

