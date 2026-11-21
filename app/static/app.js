"use strict";
const $ = id => document.getElementById(id);
const page = document.body.dataset.page;
let samples=[], selected=null, current=null, allRuns=[], launchPending=false, advice=null;
const stateNames={running:"运行中",completed:"已完成",failed:"失败",interrupted:"已停止",orphaned:"需检查"};
const phaseNames={queued:"准备实验",starting_server:"正在加载模型…",warmup:"预热中…",measuring:"正在采集与测量",finished:"实验已结束"};
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
function renderLive() {
  const s = samples.at(-1); if (!s) return;
  const age = Date.now() - Date.parse(s.timestamp);
  $("connection").textContent = age > 15000 ? "采样已过期，请检查服务" : "平台已连接 · 每 2 秒采样";
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
  $("sources").replaceChildren();
  const labels = {system:"系统遥测",gpu:"GPU 遥测",kv:"vLLM 指标",sched:"eBPF 调度"};
  for (const [key,status] of Object.entries(s.source_status)) {
    const item=document.createElement("span"); item.className="source-item "+status.status;
    item.title=status.error || "正常采集"; item.append(document.createElement("b"),document.createTextNode(labels[key]+" · "+({ok:"正常",failed:"失败",unavailable:"未接入"}[status.status] || status.status)));
    $("sources").append(item);
  }
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

function numeric(value,digits=2){return Number.isFinite(value)?Number(value.toFixed(digits)):"—";}
function renderCurrent(){
 current=allRuns.find(r=>r.status==="running")||allRuns[0]||null;
 $("start-button").disabled=launchPending||allRuns.some(r=>r.status==="running")||!$("model").value;
 $("stop-button").disabled=!current||current.status!=="running"||current.can_stop===false;
 $("inspect-button").disabled=!current;
 if(!current)return;
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
  const data=await api("/api/v1/runs/"+encodeURIComponent(id));selected=id;
  window.history.replaceState(null,"","/runs?run_id="+encodeURIComponent(id));
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
 max_tokens:Number($("max-tokens").value),gpu_kv_blocks:Number($("gpu-blocks").value),cpu_kv_blocks:Number($("cpu-blocks").value)};
}
function updatePolicy(reset=true){
 const enabled=$("policy").value==="cpu_lru";$("cpu-blocks").disabled=!enabled;$("cpu-blocks").min=enabled?"1":"0";
 if(reset)$("cpu-blocks").value=enabled?72:0;
 advice=null;$("apply-advice").disabled=true;
}
async function initExperiments(){
 const [env,catalog]=await Promise.all([api("/api/v1/environment"),api("/api/v1/strategies")]);
 for(const [id,values] of [["model",env.models],["workload",env.workloads]])for(const v of values){
  const option=document.createElement("option");option.value=v;option.textContent=v;$(id).append(option);
 }
 $("policy").replaceChildren();
 for(const strategy of catalog.strategies){const option=document.createElement("option");option.value=strategy.id;option.textContent=strategy.name;option.disabled=!strategy.available;$("policy").append(option);}
 if(!env.models.length)showError("未找到本地模型，请检查模型目录。");
 const fields={model_path:"model",workload_path:"workload",policy:"policy",requests:"requests",concurrency:"concurrency",warmup:"warmup",max_tokens:"max-tokens",gpu_kv_blocks:"gpu-blocks",cpu_kv_blocks:"cpu-blocks"};
 try{
  const draft=JSON.parse(sessionStorage.getItem("tracekv-experiment-draft")||"null");
  if(draft){
   for(const [key,id] of Object.entries(fields)){
    if(draft[key]===undefined)continue;
    if($(id).tagName==="SELECT"){
     if([...$(id).options].some(o=>o.value===draft[key]&&!o.disabled))$(id).value=draft[key];
    }else if(Number.isFinite(draft[key]))$(id).value=draft[key];
   }
   $("cpu-blocks").disabled=$("policy").value!=="cpu_lru";
   $("cpu-blocks").min=$("policy").value==="cpu_lru"?"1":"0";
   $("launch-message").textContent="已恢复本标签页的实验配置";
  }
 }catch(_){}
 const saveDraft=()=>{try{sessionStorage.setItem("tracekv-experiment-draft",JSON.stringify(formConfig()));}catch(_){}};
 $("policy").onchange=()=>{updatePolicy();saveDraft();};
 const draftChanged=()=>{advice=null;$("apply-advice").disabled=true;saveDraft();};
 $("run-form").addEventListener("change",draftChanged);
 $("run-form").addEventListener("input",draftChanged);

 $("recommend-button").onclick=async()=>{
  $("recommend-button").disabled=true;
  try{
   const snapshot=await api("/api/v1/snapshot"),cfg=formConfig();
   advice=await api("/api/v1/strategies/evaluate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({interface_version:"1.0",snapshot,current_config:{policy:cfg.policy,gpu_kv_blocks:cfg.gpu_kv_blocks,cpu_kv_blocks:cfg.cpu_kv_blocks}})});
   $("advice-text").textContent=advice.reason+" 来源："+advice.run_id+"；仅供下一轮对照。";$("apply-advice").disabled=false;
  }catch(e){showError(e.message);}finally{$("recommend-button").disabled=false;}
 };
 $("apply-advice").onclick=()=>{
  if(!advice)return;const patch=advice.config_patch;
  $("policy").value=patch.policy;$("gpu-blocks").value=patch.gpu_kv_blocks;$("cpu-blocks").value=patch.cpu_kv_blocks;
  $("cpu-blocks").disabled=patch.policy!=="cpu_lru";saveDraft();$("apply-advice").disabled=true;$("advice-text").textContent+=" 已填入，点击启动实验才会运行。";
 };
 $("run-form").onsubmit=async e=>{
  e.preventDefault();saveDraft();showError("");launchPending=true;$("start-button").disabled=true;$("launch-message").textContent="正在创建实验…";
  try{const run=await api("/api/v1/runs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(formConfig())});$("launch-message").textContent="已创建 "+run.run_id;}
  catch(e){showError(e.message);$("launch-message").textContent="启动失败";}
  finally{launchPending=false;}
 };
 $("stop-button").onclick=async()=>{
  if(!current)return;$("stop-button").disabled=true;
  try{await api("/api/v1/runs/"+current.run_id+"/stop",{method:"POST"});$("job-phase").textContent="正在停止并保存数据…";}catch(e){showError(e.message);}
 };
 $("inspect-button").onclick=()=>current&&location.assign("/runs?run_id="+encodeURIComponent(current.run_id));
}
async function init(){
 try{
  if(page==="experiments")await initExperiments();
  if(page==="monitor"){
   const [catalog,integrations]=await Promise.all([api("/api/v1/strategies"),api("/api/v1/integrations")]);
   $("strategy-bridge").textContent=catalog.strategies.filter(s=>s.available).map(s=>s.name).join("、")+" 已就绪；配置在实验工作台。";
   $("ebpf-bridge").textContent=integrations.ebpf.message+(integrations.ebpf.bpftrace_found?" 本机已找到 bpftrace。":" 本机尚未找到 bpftrace。");
   window.addEventListener("resize",drawCharts);
  }
  if(page==="runs"){
   $("run-search").oninput=renderArchive;$("run-status-filter").onchange=renderArchive;
   $("close-detail").onclick=()=>{$("detail").hidden=true;selected=null;window.history.replaceState(null,"","/runs");};
   $("reanalyze-button").onclick=async()=>{if(!selected)return;$("reanalyze-button").disabled=true;try{await api("/api/v1/runs/"+encodeURIComponent(selected)+"/analyze",{method:"POST"});await inspect(selected,false);}catch(e){showError(e.message);}finally{$("reanalyze-button").disabled=false;}};
   selected=new URLSearchParams(location.search).get("run_id");
  }
 }catch(e){showError(e.message);}
 poll();
}
async function poll(){
 try{
  if(page==="monitor"){const data=await api("/api/v1/history");samples=data.samples;renderLive();}
  else{
   allRuns=(await api("/api/v1/runs")).runs;
   $("connection").textContent="平台已连接 · 每 2 秒更新";
   if(page==="experiments")renderCurrent();
   if(page==="runs"){renderArchive();if(selected)await inspect(selected,false);}
  }
 }catch(e){$("connection").textContent="连接中断 · 自动重试";showError(e.message);}
 finally{setTimeout(poll,2000);}
}
init();
