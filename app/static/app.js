"use strict";
const $ = id => document.getElementById(id);
const page = document.body.dataset.page;
let contextId=null, contextVersion=0, timelineCursor=null, timelineData=null, timelineFilters={}, comparisonId=null, comparisonsList=[], appliedAdvice=null;
try{const q=new URLSearchParams(location.search);contextId=q.has("run_id")?q.get("run_id"):sessionStorage.getItem("tracekv-run-context");if(contextId==="host")contextId=null;}catch(_){}
let samples=[], selected=null, current=null, allRuns=[], launchPending=false, advice=null;
const stateNames={running:"运行中",completed:"已完成",failed:"失败",interrupted:"已停止",orphaned:"需检查"};
const phaseNames={queued:"准备实验",starting_server:"正在加载模型…",warmup:"预热中…",measuring:"正在采集与测量",finished:"实验已结束",stopping:"正在停止并保存数据…"};
async function api(path,options){
 const response=await fetch(path,options),data=await response.json();
 if(!response.ok)throw new Error(typeof data.detail==="string"?data.detail:JSON.stringify(data.detail));
 return data;
}
function showError(message){$("error").hidden=!message;$("error").textContent=message;}
function value(id, number, unit, digits=1) {
  $(id).replaceChildren(document.createTextNode(Number.isFinite(number) ? number.toFixed(digits) : "—"));
  const small = document.createElement("small"); small.textContent = unit; $(id).append(small);
}
function renderSources(statuses){
 $("sources").replaceChildren();
 const labels={system:"系统遥测",gpu:"GPU 遥测",kv:"vLLM 指标",sched:"eBPF 调度"};
 for(const [key,status] of Object.entries(statuses||{})){
  const item=document.createElement("span");item.className="source-item "+status.status;
  item.title=status.error||status.target_tracking||"正常采集";
  item.append(document.createElement("b"),document.createTextNode((labels[key]||key)+" · "+({ok:"正常",failed:"失败",unavailable:"未接入",disabled:"已关闭",starting:"正在尝试挂载"}[status.status]||status.status)));
  $("sources").append(item);
 }
}
function renderLive() {
  const run=allRuns.find(r=>r.run_id===contextId);
  $("view-mode").textContent=!contextId?"整机":run?.config.trace_profile==="off"?"采样关闭":run?.status==="running"?"运行":"历史";
  const s = samples.at(-1); if (!s) {
    for(const [id,unit] of [["gpu-value","MB"],["memory-value","GB"],["kv-value","%"],["hit-value","%"],["cpu-value","%"],["pressure-value","%"],["io-value","MB/s"],["pcie-value","MB/s"]])value(id,null,unit);
    $("gpu-meter").style.width="0";$("kv-meter").style.width="0";
    for(const id of ["gpu-note","running-note","cpu-note","pressure-note","io-note","pcie-note"])$(id).textContent="该实验无可用采样";
    $("sample-time").textContent="无采样";$("sources").replaceChildren();drawCharts();return;
  }
  const age = Date.now() - Date.parse(s.timestamp);
  const archived=contextId&&allRuns.find(r=>r.run_id===contextId)?.status!=="running";
  $("connection").textContent=archived?"实验历史快照 · "+new Date(s.timestamp).toLocaleString():age>15000?"采样暂停或已过期 · 查看实验状态":"平台已连接 · 每 2 秒采样";
  $("sample-time").textContent = new Date(s.timestamp).toLocaleTimeString();
  value("gpu-value",s.gpu.memory_used_mb,"MB",0);
  value("memory-value",s.system.mem_available_mb == null ? null : s.system.mem_available_mb/1000,"GB",2);
  value("kv-value",s.kv.usage_ratio == null ? null : s.kv.usage_ratio*100,"%");
  value("hit-value",s.kv.prefix_hit_ratio == null ? null : s.kv.prefix_hit_ratio*100,"%");
  $("gpu-meter").style.width = s.gpu.memory_total_mb ? Math.min(100,s.gpu.memory_used_mb/s.gpu.memory_total_mb*100)+"%" : "0";
  $("kv-meter").style.width = s.kv.usage_ratio == null ? "0" : Math.min(100,s.kv.usage_ratio*100)+"%";
  $("gpu-note").textContent = s.gpu.memory_total_mb ? "总计 "+s.gpu.memory_total_mb.toFixed(0)+" MB · 利用率 "+(s.gpu.utilization_pct ?? "—")+"%" : "GPU 数据不可用";
  $("running-note").textContent = "活跃请求："+(s.kv.running_requests ?? "—");
  const cpu = s.system.cpu || {}, psi = s.system.psi || {}, io = s.system.io || {};
  const format = (n, digits=1) => Number.isFinite(n) ? n.toFixed(digits) : "—";
  value("cpu-value",cpu.utilization_pct,"%");
  value("pressure-value",psi.psi_cpu_some_avg10,"%");
  value("io-value",io.read_bytes_per_s == null ? null : io.read_bytes_per_s/1e6,"MB/s",2);
  value("pcie-value",s.gpu.pcie_rx_kb_per_s == null ? null : s.gpu.pcie_rx_kb_per_s/1000,"MB/s",2);
  $("cpu-note").textContent = "I/O wait "+format(cpu.iowait_pct)+"% · 1 分钟负载 "+format(cpu.loadavg_1m,2);
  $("pressure-note").textContent = "内存 PSI "+format(psi.psi_mem_some_avg10)+"% · I/O PSI "+format(psi.psi_io_some_avg10)+"%";
  $("io-note").textContent = "写入 "+format(io.write_bytes_per_s == null ? null : io.write_bytes_per_s/1e6,2)+" MB/s";
  $("pcie-note").textContent = "发送 "+format(s.gpu.pcie_tx_kb_per_s == null ? null : s.gpu.pcie_tx_kb_per_s/1000,2)+" MB/s · 周期采样可能遗漏突发";
  renderSources(s.source_status);
  drawCharts();
}
function chart(id, series, maxValue) {
  const canvas=$(id), box=canvas.getBoundingClientRect(), ratio=devicePixelRatio||1;
  if (!box.width || !box.height) return;
  canvas.width=box.width*ratio; canvas.height=box.height*ratio;
  const ctx=canvas.getContext("2d"); ctx.scale(ratio,ratio);
  const w=box.width,h=box.height,pad={l:45,r:12,t:12,b:28};
  const values=series.flatMap(s=>samples.map(s.get)).filter(Number.isFinite);
  const max=maxValue || Math.max(1,...values)*1.12;
  ctx.font="12px Segoe UI"; ctx.fillStyle="#72899b"; ctx.strokeStyle="#23303d"; ctx.lineWidth=1;
  for(let i=0;i<5;i++){
    const y=pad.t+(h-pad.t-pad.b)*i/4;
    ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();
    ctx.fillText((max*(1-i/4)).toFixed(0),2,y+3);
  }
  if(!values.length){ctx.fillText("等待真实数据 · 无可用采样",pad.l+20,h/2);return;}
  const first=samples.length ? Date.parse(samples[0].timestamp) : 0;
  const last=samples.length ? Date.parse(samples.at(-1).timestamp) : first;
  const duration=Math.max(2000,last-first);
  for(const s of series){
    ctx.strokeStyle=s.color;ctx.lineWidth=1.8;ctx.beginPath();let moving=false;
    for(const item of samples){
      const val=s.get(item);
      if(!Number.isFinite(val)){moving=false;continue;}
      const x=pad.l+(w-pad.l-pad.r)*(Date.parse(item.timestamp)-first)/duration;
      const y=h-pad.b-(h-pad.t-pad.b)*val/max;
      if(moving)ctx.lineTo(x,y);else ctx.moveTo(x,y); moving=true;
    }ctx.stroke();
  }
  if(samples.length){ctx.fillStyle="#72899b";ctx.fillText(new Date(first).toLocaleTimeString(),pad.l,h-5);ctx.fillText(new Date(last).toLocaleTimeString(),w-65,h-5);}
}
function drawCharts(){
  chart("resource-chart",[{color:"#67e6bf",get:s=>s.gpu.memory_used_mb},{color:"#77baff",get:s=>s.system.mem_available_mb}]);
  chart("kv-chart",[{color:"#b19aff",get:s=>s.kv.usage_ratio==null?null:s.kv.usage_ratio*100},{color:"#67e6bf",get:s=>s.kv.prefix_hit_ratio==null?null:s.kv.prefix_hit_ratio*100}],100);
}
function pill(status){const span=document.createElement("span");span.className="pill "+status;span.textContent=stateNames[status]||status;return span;}

const assessmentNames={insufficient_data:"证据不足",incomparable_configuration:"配置或版本不一致，暂不可比",paired_observation_requires_repeats:"已记录，需多轮验证"};
function numeric(value,digits=2){return Number.isFinite(value)?Number(value.toFixed(digits)):"—";}
function renderCurrent(){
 current=(contextId?allRuns.find(r=>r.run_id===contextId):null)||allRuns.find(r=>r.status==="running")||null;
 $("start-button").disabled=launchPending||allRuns.some(r=>r.status==="running")||comparisonsList.some(c=>c.status==="running"||c.status==="external_running")||!$("model").value;
 $("stop-button").disabled=!current||current.status!=="running"||current.can_stop===false;
 $("inspect-button").disabled=!current;
 if(!current){$("job-id").textContent="未选择实验";$("job-phase").textContent="准备好开始实验";$("job-detail").textContent="启动新实验或在上方选择已保存的运行。";$("job-progress").style.width="0";$("job-status").textContent="空闲";return;}
 $("job-id").textContent=current.run_id;
 $("job-status").replaceWith(Object.assign(pill(current.status),{id:"job-status"}));
 $("job-phase").textContent=phaseNames[current.phase]||current.phase;
 $("job-detail").textContent=current.error||("已记录 "+(current.recorded_requests||0)+" / "+current.config.requests+" 次正式请求");
 $("job-progress").style.width=Math.min(100,(current.recorded_requests||0)/current.config.requests*100)+"%";
}
function renderArchive(){
 const query=$("run-search").value.trim().toLowerCase(),status=$("run-status-filter").value;
 const runs=allRuns.filter(r=>(!query||(r.run_id+" "+r.config.policy).toLowerCase().includes(query))&&(!status||r.status===status));
 $("archive-count").textContent="显示 "+runs.length+" / "+allRuns.length+" 次实验";
 $("run-list").replaceChildren();
 if(!runs.length){
  const tr=document.createElement("tr"),td=document.createElement("td");td.colSpan=5;td.className="empty";
  td.textContent=allRuns.length?"没有匹配的实验记录。":"暂无实验记录，请在实验工作台创建任务。";tr.append(td);$("run-list").append(tr);
 }
 for(const run of runs){
  const tr=document.createElement("tr");
  for(const text of [run.run_id,run.config.policy]){const td=document.createElement("td");td.textContent=text;tr.append(td);}
  const status=document.createElement("td");status.append(pill(run.status));tr.append(status);
  const count=document.createElement("td");count.textContent=(run.recorded_requests||0)+" / "+run.config.requests;tr.append(count);
  const td=document.createElement("td"),button=document.createElement("button");button.className="secondary";button.textContent="查看详情";button.onclick=()=>inspect(run.run_id,true);td.append(button);tr.append(td);$("run-list").append(tr);
 }
}
function renderTelemetry(telemetry){
 $("telemetry-detail").replaceChildren();
 for(const [source,label] of [["cache","外部缓存事件"],["ebpf","外部 eBPF 事件"]]){
  const state=telemetry?.sources?.[source],box=document.createElement("article");box.className="telemetry-card";
  const heading=document.createElement("h4");heading.textContent=label;box.append(heading);
  const text=document.createElement("p");
  text.textContent=state?.batch_count?("已接收 "+state.event_count+" 条事件 / "+state.batch_count+" 个批次；这是实验记录，不代表当前正在采集。"):"尚未通过新接口提交此实验的真实批次；已有框架指标与迁移文件可在实验产物中查看。";box.append(text);
  for(const group of state?.groups||[]){
   const line=document.createElement("p");line.textContent="采集器 "+group.collector_id+" · 范围 "+(group.scope||"cache")+" · 丢失 "+(group.dropped_events??"未知");box.append(line);
   for(const [metric,stats] of Object.entries(group.metrics)){
    const row=document.createElement("p");row.textContent=metric+"："+stats.count+" 次 · P95 "+numeric(stats.p95_us)+" μs";box.append(row);
   }
  }
  if(state?.errors?.length){const err=document.createElement("p");err.textContent=state.errors.join("；");box.append(err);}
  $("telemetry-detail").append(box);
 }
}
async function inspect(id,scroll=false){
 try{
  if(contextId!==id)setContext(id,false);
  const version=contextVersion;
  const data=await api("/api/v1/runs/"+encodeURIComponent(id));if(version!==contextVersion)return;selected=id;
  $("detail-id").textContent=id;$("artifacts").replaceChildren();$("detail-metrics").replaceChildren();
  for(const filename of data.artifacts){
   const a=document.createElement("a");a.textContent=filename;a.href="/api/v1/runs/"+encodeURIComponent(id)+"/files/"+filename;a.target="_blank";$("artifacts").append(a);
  }
  for(const [label,key] of [["成功请求","successful_requests"],["TTFT P95 / ms","client_ttft_ms_p95"],["E2E P95 / ms","client_e2e_ms_p95"]]){
   const el=document.createElement("span");el.textContent=label+"："+numeric(data.summary?.[key]);$("detail-metrics").append(el);
  }
  const names={HOST_SCHEDULING_CONTENTION:"CPU 调度压力偏高",SYSTEM_MEMORY_IO_PRESSURE:"内存或 I/O 压力偏高",GPU_HIGH_UTILIZATION:"GPU 活动水平较高",HIGH_PCIE_UTILIZATION:"PCIe 采样流量较高",NO_RULE_TRIGGERED:"当前指标未触发调查规则",INSUFFICIENT_DATA:"缺少判断所需指标"};
  $("diagnosis").hidden=!data.diagnosis;
  if(data.diagnosis){
   $("diagnosis-title").textContent=names[data.diagnosis.primary_bottleneck]||data.diagnosis.primary_bottleneck;
   $("diagnosis-note").textContent="这是待验证的调查提示，尚未建立请求变慢的因果关系。";
   const counts=data.diagnosis.source_coverage||{};
   $("diagnosis-coverage").textContent="有效采样：GPU "+(counts.gpu??0)+" · CPU PSI "+(counts.psi_cpu??0)+" · 调度 "+(counts.sched??0);
  }
  const slo=data.summary?.slo;
  for(const text of ["客户端 TPOT 估计 P95 / ms："+numeric(data.summary?.client_tpot_estimate_ms_p95),slo?.configured?("SLO 达标："+slo.passed+" / "+slo.planned_requests+" 计划 · 未知 "+slo.unknown+" · 达标吞吐 "+numeric(slo.goodput_requests_per_second)+" 请求/s"):"SLO：本次未配置","建议复核："+(assessmentNames[data.analysis?.recommendation_assessment?.status]||data.analysis?.recommendation_assessment?.status||"未关联建议")]){
   const el=document.createElement("span");el.textContent=text;$("detail-metrics").append(el);
  }
  renderTelemetry(data.telemetry);
  $("report-freshness").textContent=data.report_outdated?"报告尚未纳入最新接入批次。":"接入数据与已有报告分别保留；旧实验无外部数据时不补造数值。";
  $("reanalyze-button").hidden=!data.report_outdated||data.manifest.status==="running";
  $("detail-text").textContent=JSON.stringify({manifest:data.manifest,analysis:data.analysis},null,2);
  $("detail").hidden=false;if(scroll)$("detail").scrollIntoView({behavior:"smooth",block:"start"});
 }catch(e){showError(e.message);}
}
function formConfig(){
 return {model_path:$("model").value,workload_path:$("workload").value,policy:$("policy").value,
 requests:Number($("requests").value),concurrency:Number($("concurrency").value),warmup:Number($("warmup").value),
 max_tokens:Number($("max-tokens").value),gpu_kv_blocks:Number($("gpu-blocks").value),cpu_kv_blocks:Number($("cpu-blocks").value),
 trace_profile:$("trace-profile").value,seed:Number($("seed").value),
 slo_ttft_ms:$("slo-ttft").value?Number($("slo-ttft").value):null,
 slo_e2e_ms:$("slo-e2e").value?Number($("slo-e2e").value):null,
 slo_tpot_ms:$("slo-tpot").value?Number($("slo-tpot").value):null};
}
function updatePolicy(reset=true){
 const enabled=$("policy").value==="cpu_lru";$("cpu-blocks").disabled=!enabled;$("cpu-blocks").min=enabled?"1":"0";
 if(reset)$("cpu-blocks").value=enabled?72:0;
 advice=null;appliedAdvice=null;$("apply-advice").disabled=true;
}
async function initExperiments(){
 const [env,catalog,integration]=await Promise.all([api("/api/v1/environment"),api("/api/v1/strategies"),api("/api/v1/integrations")]);
 const caps=integration.ebpf;
 const canAttempt=caps.can_attempt===true;
 $("trace-profile").querySelector('option[value="ebpf"]').disabled=!canAttempt;
 $("batch-ebpf").disabled=!canAttempt;
 $("collector-capability").textContent=canAttempt?"可尝试 eBPF；实际挂载与数据质量以每轮记录为准。":"eBPF 未就绪："+(caps.reason||caps.message)+"。普通采样与外部批次接入可用。";
 for(const [id,values] of [["model",env.models],["workload",env.workloads]])for(const v of values){
  const option=document.createElement("option");option.value=v;option.textContent=v;$(id).append(option);
 }
 $("policy").replaceChildren();
 for(const strategy of catalog.strategies){const option=document.createElement("option");option.value=strategy.id;option.textContent=strategy.name;option.disabled=!strategy.available;$("policy").append(option);}
 if(!env.models.length)showError("未找到本地模型，请检查模型目录。");
 const fields={model_path:"model",workload_path:"workload",policy:"policy",requests:"requests",concurrency:"concurrency",warmup:"warmup",max_tokens:"max-tokens",gpu_kv_blocks:"gpu-blocks",cpu_kv_blocks:"cpu-blocks",trace_profile:"trace-profile",seed:"seed",slo_ttft_ms:"slo-ttft",slo_e2e_ms:"slo-e2e",slo_tpot_ms:"slo-tpot"};
 try{
  const draft=JSON.parse(sessionStorage.getItem("tracekv-experiment-draft")||"null");
  if(draft){
   for(const [key,id] of Object.entries(fields)){
    if(draft[key]===undefined)continue;
    if($(id).tagName==="SELECT"){
     if([...$(id).options].some(o=>o.value===draft[key]&&!o.disabled))$(id).value=draft[key];
    }else if(Number.isFinite(draft[key])||draft[key]===null)$(id).value=draft[key]??"";
   }
   $("cpu-blocks").disabled=$("policy").value!=="cpu_lru";
   $("cpu-blocks").min=$("policy").value==="cpu_lru"?"1":"0";
   appliedAdvice=draft.recommendation_id||null;
   $("launch-message").textContent=appliedAdvice?"已恢复实验草稿及建议关联":"已恢复本标签页的实验配置";
  }
 }catch(_){}
 const saveDraft=()=>{try{sessionStorage.setItem("tracekv-experiment-draft",JSON.stringify({...formConfig(),recommendation_id:appliedAdvice}));}catch(_){}};
 $("policy").onchange=()=>{updatePolicy();saveDraft();};
 const draftChanged=()=>{advice=null;appliedAdvice=null;$("apply-advice").disabled=true;saveDraft();};
 $("run-form").addEventListener("change",draftChanged);
 $("run-form").addEventListener("input",draftChanged);

 $("recommend-button").onclick=async()=>{
  $("recommend-button").disabled=true;
  try{
   if(!contextId)throw new Error("请先选择作为建议依据的具体实验。");
   const [snapshot,source]=await Promise.all([api("/api/v1/runs/"+encodeURIComponent(contextId)+"/snapshot"),api("/api/v1/runs/"+encodeURIComponent(contextId))]),cfg=source.manifest.config;
   advice=await api("/api/v1/strategies/evaluate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({interface_version:"1.0",snapshot,current_config:{policy:cfg.policy,gpu_kv_blocks:cfg.gpu_kv_blocks,cpu_kv_blocks:cfg.cpu_kv_blocks}})});
   $("advice-text").textContent=advice.reason+" 来源："+advice.run_id+"；来源记录"+(advice.source_run_verified?"已核对":"未核对")+"，仅供下一轮对照。";$("apply-advice").disabled=!advice.source_run_verified;
  }catch(e){showError(e.message);}finally{$("recommend-button").disabled=false;}
 };
 $("apply-advice").onclick=()=>{
  if(!advice)return;appliedAdvice=advice.recommendation_id;const patch=advice.config_patch;
  $("policy").value=patch.policy;$("gpu-blocks").value=patch.gpu_kv_blocks;$("cpu-blocks").value=patch.cpu_kv_blocks;
  $("cpu-blocks").disabled=patch.policy!=="cpu_lru";saveDraft();$("apply-advice").disabled=true;$("advice-text").textContent+=" 已填入，点击启动实验才会运行。";
 };
 $("run-form").onsubmit=async e=>{
  e.preventDefault();saveDraft();showError("");launchPending=true;$("start-button").disabled=true;$("launch-message").textContent="正在创建实验…";
  try{const run=await api("/api/v1/runs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({...formConfig(),...(appliedAdvice?{recommendation_id:appliedAdvice}:{})})});setContext(run.run_id);$("launch-message").textContent="已创建 "+run.run_id;}
  catch(e){showError(e.message);$("launch-message").textContent="启动失败";}
  finally{launchPending=false;}
 };
 $("stop-button").onclick=async()=>{
  if(!current)return;$("stop-button").disabled=true;
  try{await api("/api/v1/runs/"+current.run_id+"/stop",{method:"POST"});$("job-phase").textContent="正在停止并保存数据…";}catch(e){showError(e.message);}
 };
 $("inspect-button").onclick=()=>current&&location.assign("/runs?run_id="+encodeURIComponent(current.run_id));
 $("recommend-button").disabled=false;
}
async function init(){
 try{
  $("run-context").onchange=()=>setContext($("run-context").value||null);
  updateContextLinks();
  if($("comparison-select"))initComparisonControls();
  if(page==="experiments")await initExperiments();
  if(page==="monitor"){
   const [catalog,integrations]=await Promise.all([api("/api/v1/strategies"),api("/api/v1/integrations")]);
   $("strategy-bridge").textContent=catalog.strategies.filter(s=>s.available).map(s=>s.name).join("、")+" 已就绪；配置在实验工作台。";
   $("ebpf-bridge").textContent=integrations.ebpf.message+(integrations.ebpf.bpftrace_found?" 本机已找到 bpftrace。":" 本机尚未找到 bpftrace。");
   window.addEventListener("resize",()=>{drawCharts();drawEvents();});initTimelineControls();
  }
  if(page==="runs"){
   $("run-search").oninput=renderArchive;$("run-status-filter").onchange=renderArchive;
   $("close-detail").onclick=()=>{$("detail").hidden=true;selected=null;setContext(null,false);};
   $("reanalyze-button").onclick=async()=>{if(!selected)return;$("reanalyze-button").disabled=true;try{await api("/api/v1/runs/"+encodeURIComponent(selected)+"/analyze",{method:"POST"});await inspect(selected,false);}catch(e){showError(e.message);}finally{$("reanalyze-button").disabled=false;}};
   selected=contextId;
  }
 }catch(e){showError(e.message);}
 poll();
}

function updateContextLinks(){
 for(const a of document.querySelectorAll('nav a,.brand')){
  const path=new URL(a.href,location.href).pathname;
  a.href=path+"?run_id="+encodeURIComponent(contextId||"host");
 }
 const query=new URLSearchParams(location.search);query.set("run_id",contextId||"host");window.history.replaceState(null,"",location.pathname+"?"+query);
}
function setContext(id,refresh=true){
 contextId=id;contextVersion++;timelineCursor=null;timelineData=null;timelineFilters={};advice=null;if($("apply-advice"))$("apply-advice").disabled=true;if(page==="monitor"){$("timeline-start").value="";$("timeline-end").value="";}
 try{sessionStorage.setItem("tracekv-run-context",id||"host");}catch(_){}
 updateContextLinks();renderContexts();
 if(page==="runs"){selected=id;if(!id)$("detail").hidden=true;}
 if(refresh&&page==="monitor"){samples=[];renderLive();refreshMonitor().catch(e=>showError(e.message));}
 if(refresh&&page==="runs"&&id)inspect(id,false);
 if(page==="experiments")renderCurrent();
}
function renderContexts(){
 const el=$("run-context"),value=contextId||"";
 const options=[Object.assign(document.createElement("option"),{value:"",textContent:"整机视图 · 不归属某次实验"})];
 for(const run of allRuns)options.push(Object.assign(document.createElement("option"),{value:run.run_id,textContent:run.run_id+" · "+(stateNames[run.status]||run.status)+" · "+run.config.policy+" / "+(run.config.trace_profile||"sampling")}));
 if(contextId&&!allRuns.some(r=>r.run_id===contextId))options.push(Object.assign(document.createElement("option"),{value:contextId,textContent:contextId+" · 正在核对"}));
 if([...el.options].map(o=>o.value+"|"+o.textContent).join("\n")!==options.map(o=>o.value+"|"+o.textContent).join("\n"))el.replaceChildren(...options);el.value=value;
 $("context-note").textContent=contextId?"三页共同查看 "+contextId+"；GPU 为整卡、系统指标为整机范围。":"整机视图；自动实验期间后台整机采样暂停，历史值会标注过期。";
}
function fillRow(tbody,values){
 const tr=document.createElement("tr");
 for(const text of values){const td=document.createElement("td");td.textContent=text;tr.append(td);}
 tbody.append(tr);return tr;
}
async function refreshMonitor(){
 const version=contextVersion,id=contextId;
 const data=await api(id?"/api/v1/runs/"+encodeURIComponent(id)+"/history":"/api/v1/history");
 if(version!==contextVersion)return;
 samples=data.samples;renderLive();if(!samples.length)renderSources(data.source_status||{});
 if(!samples.length)$("connection").textContent=id?"该实验未采样 · 查看采集配置或状态":"等待整机真实采样";
 if(id)await refreshTimeline(version);
 else{timelineData=null;$("timeline-status").textContent="请选择具体实验查看时间线。";$("timeline-events").replaceChildren();$("stage-rows").replaceChildren();$("timeline-quality").textContent="整机视图不混入任何实验的请求与事件。";$("timeline-page").textContent="";$("timeline-prev").disabled=true;$("timeline-next").disabled=true;drawEvents();}
}
function initTimelineControls(){
 $("timeline-filter").onsubmit=e=>{e.preventDefault();showError("");timelineFilters={};for(const [id,key] of [["timeline-start","start"],["timeline-end","end"]])if($(id).value)timelineFilters[key]=new Date($(id).value).toISOString();timelineCursor=0;refreshMonitor().catch(e=>showError(e.message));};
 $("timeline-reset").onclick=()=>{$("timeline-start").value="";$("timeline-end").value="";timelineFilters={};timelineCursor=null;showError("");refreshMonitor().catch(e=>showError(e.message));};
 for(const [id,key] of [["timeline-prev","previous_cursor"],["timeline-next","next_cursor"]])$(id).onclick=()=>{timelineCursor=timelineData?.[key];refreshMonitor().catch(e=>showError(e.message));};
}
async function refreshTimeline(version){
 const q=new URLSearchParams({limit:"200"});
 for(const [key,value] of Object.entries(timelineFilters))q.set(key,value);
 if(timelineCursor!==null)q.set("cursor",timelineCursor);
 const data=await api("/api/v1/runs/"+encodeURIComponent(contextId)+"/timeline?"+q);
 if(version!==contextVersion)return;
 timelineData=data;
 $("timeline-status").textContent=(stateNames[data.status]||data.status)+" · 全部来源记录 "+JSON.stringify(data.source_event_counts)+" · 无法解析 "+data.invalid_records;
 $("timeline-quality").textContent=JSON.stringify({clock_anchor:data.clock_anchor,source_status:data.source_status,limitations:data.limitations},null,2);
 $("timeline-events").replaceChildren();
 for(const event of data.events.filter(e=>e.source!=="snapshot")){
  const d=event.data;fillRow($("timeline-events"),[new Date(event.timestamp).toLocaleTimeString("zh-CN",{hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false,fractionalSecondDigits:3}),event.source+" / "+event.metric,
   "请求 "+(d.request_id??"未知")+" · "+(d.scope||"请求记录")+(d.tid?" · TID "+d.tid:""),
   event.source==="request"?"E2E "+numeric(d.e2e_ms)+" ms · "+(d.success?"成功":"失败"):d.value!==undefined?numeric(d.value)+" "+(d.unit||""):"方向 "+(d.direction||"—")+" · "+(d.status||"事件")]);
 }
 if(!$("timeline-events").children.length)fillRow($("timeline-events"),["—","当前页无请求 / 跨层事件","缺失保持未知","不补零"]);
 $("timeline-page").textContent="记录 "+(data.total_events?data.cursor+1:0)+"–"+Math.min(data.cursor+data.events.length,data.total_events)+" / "+data.total_events;
 $("timeline-prev").disabled=data.previous_cursor===null;$("timeline-next").disabled=data.next_cursor===null;
 $("stage-rows").replaceChildren();
 for(const h of samples.at(-1)?.kv?.stage_histograms||[]){
  const d=h.delta;fillRow($("stage-rows"),[h.name,JSON.stringify(h.labels),h.reset?"计数器重置":d?.count??"等待差分",d&&d.count&&d.sum!==null?numeric(d.sum/d.count*1000):"—"]);
 }
 if(!$("stage-rows").children.length)fillRow($("stage-rows"),["未获取阶段直方图","不跨标签合并","—","—"]);
 drawEvents();
}
function drawEvents(){
 const canvas=$("event-chart");if(!canvas)return;
 const box=canvas.getBoundingClientRect(),ratio=devicePixelRatio||1;if(!box.width)return;
 canvas.width=box.width*ratio;canvas.height=box.height*ratio;
 const ctx=canvas.getContext("2d");ctx.scale(ratio,ratio);
 const rows=[["request","请求","#67e6bf"],["cache_hook","缓存迁移","#77baff"],["cache","外部缓存","#b19aff"],["ebpf","eBPF","#f2b177"]];
 ctx.font="12px Segoe UI";const events=timelineData?.events.filter(e=>e.source!=="snapshot")||[];
 const times=events.flatMap(e=>[e.epoch_s,e.end_timestamp?Date.parse(e.end_timestamp)/1000:e.epoch_s]);
 const left=Math.min(...times),right=Math.max(...times),span=Math.max(.001,right-left);
 rows.forEach(([source,label,color],i)=>{
  const y=30+i*32;ctx.fillStyle="#8b9bab";ctx.fillText(label,0,y+3);ctx.strokeStyle="#25303c";ctx.beginPath();ctx.moveTo(76,y);ctx.lineTo(box.width,y);ctx.stroke();
  ctx.fillStyle=color;
  for(const e of events.filter(e=>e.source===source)){
   const x=80+(box.width-90)*(e.epoch_s-left)/span;
   const end=e.end_timestamp?80+(box.width-90)*(Date.parse(e.end_timestamp)/1000-left)/span:x;
   ctx.fillRect(x,y-4,Math.max(2,end-x),8);
  }
 });
 ctx.fillStyle="#8b9bab";
 if(events.length){ctx.fillText(new Date(left*1000).toLocaleTimeString(),80,174);ctx.fillText(new Date(right*1000).toLocaleTimeString(),Math.max(80,box.width-76),174);}
 else ctx.fillText("无事件 · 选择实验后查看真实记录",80,174);
}
function initComparisonControls(){
 comparisonId=new URLSearchParams(location.search).get("comparison_id");
 $("comparison-select").onchange=()=>{comparisonId=$("comparison-select").value||null;refreshComparisons().catch(e=>showError(e.message));};
 $("batch-stop").onclick=async()=>{try{await api("/api/v1/comparisons/"+encodeURIComponent(comparisonId)+"/stop",{method:"POST"});$("batch-stop").disabled=true;}catch(e){showError(e.message);}};
 if($("batch-form"))$("batch-form").onsubmit=async e=>{
  e.preventDefault();showError("");
  if(!$("run-form").reportValidity())return;
  const profiles=["off","sampling","ebpf"].filter(x=>$("batch-"+x).checked);
  $("batch-start").disabled=true;
  try{
   const result=await api("/api/v1/comparisons",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({
    base_config:formConfig(),policies:["gpu_only","cpu_lru"],trace_profiles:profiles,repetitions:Number($("batch-repeat").value),
    cpu_kv_blocks:Number($("batch-cpu").value),order_seed:Number($("batch-seed").value)})});
   comparisonId=result.comparison_id;$("batch-message").textContent="已创建 "+comparisonId;await refreshComparisons();
  }catch(e){showError(e.message);}finally{$("batch-start").disabled=false;}
 };
}
async function refreshComparisons(){
 if(!$("comparison-select"))return;
 comparisonsList=(await api("/api/v1/comparisons")).comparisons;
 if(!comparisonId)comparisonId=comparisonsList[0]?.comparison_id||null;
 const options=comparisonsList.map(c=>Object.assign(document.createElement("option"),{value:c.comparison_id,textContent:c.comparison_id+" · "+(stateNames[c.status]||c.status)}));
 if(!options.length)options.push(Object.assign(document.createElement("option"),{value:"",textContent:"暂无对照记录"}));
 const select=$("comparison-select");if([...select.options].map(o=>o.value+"|"+o.textContent).join("\n")!==options.map(o=>o.value+"|"+o.textContent).join("\n"))select.replaceChildren(...options);select.value=comparisonId||"";
 if($("batch-start"))$("batch-start").disabled=launchPending||allRuns.some(r=>r.status==="running")||comparisonsList.some(c=>c.status==="running"||c.status==="external_running");
 if(!comparisonId)return;
 const data=await api("/api/v1/comparisons/"+encodeURIComponent(comparisonId));
 const box=$("comparison-summary"),expanded=box.querySelector("details")?.open;box.replaceChildren();
 const m=data.manifest,p=document.createElement("p");p.textContent=(stateNames[m.status]||m.status)+" · "+m.members.filter(x=>x.status==="completed").length+" / "+m.members.length+" 次完成"+(m.error?" · "+m.error:"");box.append(p);
 $("batch-stop").disabled=m.status!=="running";
 const labels={baseline:"基础对照",insufficient_data:"样本不足",observed_improvement:"观察到改善",observed_regression:"观察到退化",no_clear_gain:"未见明确收益",output_mismatch:"输出不一致",incomparable_or_unverified_identity:"身份不一致或未核验"};
 if(data.summary){
  const scroll=document.createElement("div");scroll.className="table-scroll";const table=document.createElement("table"),body=document.createElement("tbody");
  const head=document.createElement("thead");fillRow(head,["策略 / 采集范围","完成 / 失败","E2E P95 均值 ms","结论"]);table.append(head,body);
  for(const [key,c] of Object.entries(data.summary.cells))fillRow(body,[key,c.completed+" / "+c.failed_or_interrupted,numeric(c.descriptive_statistics.client_e2e_ms_p95.mean),labels[c.comparison_result]||c.comparison_result]);
  scroll.append(table);box.append(scroll);
  const info=document.createElement("p");info.textContent="每轮 P95 的描述统计；不等于合并请求的 P95。身份核验、SLO 和采集开销见完整报告。";box.append(info);
 }
 for(const file of ["report.md","summary.json","manifest.json"]){const a=document.createElement("a");a.textContent="下载 "+file;a.href="/api/v1/comparisons/"+encodeURIComponent(comparisonId)+"/files/"+file;a.target="_blank";box.append(a);}
 const details=document.createElement("details"),summary=document.createElement("summary");details.open=!!expanded;summary.textContent="各轮原始记录";details.append(summary);
 for(const member of m.members){
  const p=document.createElement("p");p.textContent="第 "+member.repeat+" 轮 · "+member.policy+" / "+member.trace_profile+" · "+(stateNames[member.status]||member.status)+(member.error?" · "+member.error:"");
  if(member.run_id){const a=document.createElement("a");a.textContent=member.run_id;a.href="/runs?run_id="+encodeURIComponent(member.run_id);p.append(a);}details.append(p);
 }box.append(details);
}
async function poll(){
 try{
  allRuns=(await api("/api/v1/runs")).runs;renderContexts();
  if(page==="monitor")await refreshMonitor();
  else{
   $("connection").textContent="平台已连接 · 每 2 秒更新";
   await refreshComparisons();
   if(page==="experiments")renderCurrent();
   if(page==="runs"){renderArchive();if(selected)await inspect(selected,false);}
  }
 }catch(e){$("connection").textContent="更新失败 · 自动重试";showError(e.message);}
 finally{setTimeout(poll,2000);}
}
init();
