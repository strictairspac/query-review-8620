import{batches}from'../stores/batch.js';import{header}from'../components/common/header.js';import{table}from'../components/common/table.js';import{badge}from'../components/common/badge.js';import{esc}from'../utils/html.js';import{SOURCES,PHASES,REASONS}from'../constants/enums.js';
const TERMINAL=['passed','different','failed'];
const sourceCell=x=>x.source?esc(SOURCES[x.source]||x.source):'<span class="muted">未执行</span>';
const reasonCell=x=>x.source==='primary'&&x.fallback_reason?esc(REASONS[x.fallback_reason]||x.fallback_reason):x.fallback_reason==='fallback_slot_timeout'?'<span class="error">'+esc(REASONS.fallback_slot_timeout)+'</span>':'<span class="muted">—</span>';
function timeline(events){if(!events?.length)return'<span class="muted">—</span>';return'<span class="phaseflow">'+events.filter(e=>e.phase!=='accepted').map(e=>esc(PHASES[e.phase]||e.phase)).join(' → ')+'</span>'}
function healthBanner(h){
  if(!h)return'';
  if(h.configured&&h.ok&&!h.paused)return`<div class="health ok"><strong>副本在线</strong> 已回放 ${esc(h.replay_lsn||'—')}；新批次追平受理前变更后才在副本执行。</div>`;
  if(h.ok&&h.paused)return`<div class="health warn"><strong>副本回放已暂停</strong> 回放位置停在 ${esc(h.replay_lsn||'—')}；新批次将等库，超时后按单一回退名额回主库。</div>`;
  if(h.configured&&!h.ok)return`<div class="health warn"><strong>副本不可用</strong> ${esc(h.error||'无法连接')}</div>`;
  return`<div class="health warn"><strong>未配置只读副本</strong> 所有批次会直接占用主库回退名额执行。</div>`;
}
export async function historyPage(el){const rows=await batches.load();const health=batches.health;el.innerHTML=header('03','运行记录按批次留痕','受理时记录主库 WAL 位置；副本回放到该位置后才在副本受理。副本落后或中途断开时整批回主库重做，主库全局只容一批回退，每批最多等十秒。')+healthBanner(health)+`<div class="panel"><div class="actions" style="margin-top:0;margin-bottom:20px"><button id="refresh">刷新队列</button><a href="#review">新建复核 →</a><span class="muted">${rows.length} 个批次 · 状态：排队 / 执行 / 完成 / 失败，过程列展示等库与回退</span></div>${table(['编号 / 批次','提交时间','运行状态','执行库','为何回主库','过程','执行证据'],rows.map(x=>['<span class="mono">#'+x.id+'</span><br>'+esc(x.name),esc(x.created_at),badge(x.status),sourceCell(x),reasonCell(x),timeline(x.events),TERMINAL.includes(x.status)?`<a href="#evidence/${x.id}">打开证据 →</a>`:'<span class="muted">后台处理中，请刷新</span>']))}</div>`;el.querySelector('#refresh').onclick=()=>historyPage(el)}
