export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'排队中',executing:'执行中',passed:'结果一致',different:'存在差异',failed:'执行失败'};
export const SOURCES={replica:'只读副本',primary:'主库回退'};
export const PHASES={accepted:'受理',waiting_replica:'等库',executing_replica:'副本执行',executing_primary:'主库执行',replica_lost:'副本断开',fallback_acquired:'取得回退名额',replica_recovered:'副本恢复',requeued:'中断恢复重排',completed:'完成',failed:'失败'};
export const REASONS={replica_lag:'副本未追上受理前的账单变更',replica_lost:'副本执行中连接断开',replica_unavailable:'副本不可用或未配置',fallback_slot_timeout:'等待主库回退名额超过十秒'};
