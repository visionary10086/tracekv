"use strict";
const $ = id => document.getElementById(id);
let history = [], selected = null, current = null, launchPending = false;
const stateNames = {running:"运行中", completed:"已完成", failed:"失败", interrupted:"已停止", orphaned:"需检查"};
const phaseNames = {queued:"准备实验", starting_server:"正在加载模型…", warmup:"预热中…", measuring:"正在采集与测量", finished:"实验已结束"};
async function api(path, options) {
  const response = await fetch(path, options);
  const data = await response.json();
  if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
  return data;
}
function value(id, number, unit, digits=1) {
  $(id).replaceChildren(document.createTextNode(Number.isFinite(number) ? number.toFixed(digits) : "—"));
  const small = document.createElement("small"); small.textContent = unit; $(id).append(small);
}
function renderLive() {
  const s = history.at(-1); if (!s) return;
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
  $("sources").replaceChildren();
  const labels = {system:"系统内存",gpu:"GPU 遥测",kv:"vLLM 指标",sched:"eBPF 调度"};
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
  const values=series.flatMap(s=>history.map(s.get)).filter(Number.isFinite);
  const max=maxValue || Math.max(1,...values)*1.12;
  ctx.font="9px Segoe UI"; ctx.fillStyle="#72899b"; ctx.strokeStyle="#23303d"; ctx.lineWidth=1;
  for(let i=0;i<5;i++){
    const y=pad.t+(h-pad.t-pad.b)*i/4;
    ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();
    ctx.fillText((max*(1-i/4)).toFixed(0),2,y+3);
  }
  if(!values.length){ctx.fillText("等待真实数据 · 无可用采样",pad.l+20,h/2);return;}
  const first=history.length ? Date.parse(history[0].timestamp) : 0;
  const last=history.length ? Date.parse(history.at(-1).timestamp) : first;
  const duration=Math.max(2000,last-first);
  for(const s of series){
    ctx.strokeStyle=s.color;ctx.lineWidth=1.8;ctx.beginPath();let moving=false;
    for(const item of history){
      const val=s.get(item);
      if(!Number.isFinite(val)){moving=false;continue;}
      const x=pad.l+(w-pad.l-pad.r)*(Date.parse(item.timestamp)-first)/duration;
      const y=h-pad.b-(h-pad.t-pad.b)*val/max;
      if(moving)ctx.lineTo(x,y);else ctx.moveTo(x,y); moving=true;
    }ctx.stroke();
  }
  if(history.length){ctx.fillStyle="#72899b";ctx.fillText(new Date(first).toLocaleTimeString(),pad.l,h-5);ctx.fillText(new Date(last).toLocaleTimeString(),w-65,h-5);}
}
function drawCharts(){
  chart("resource-chart",[{color:"#67e6bf",get:s=>s.gpu.memory_used_mb},{color:"#77baff",get:s=>s.system.mem_available_mb}]);
  chart("kv-chart",[{color:"#b19aff",get:s=>s.kv.usage_ratio==null?null:s.kv.usage_ratio*100},{color:"#67e6bf",get:s=>s.kv.prefix_hit_ratio==null?null:s.kv.prefix_hit_ratio*100}],100);
}
function pill(status){const span=document.createElement("span");span.className="pill "+status;span.textContent=stateNames[status]||status;return span;}
async function renderRuns(){
  const data=await api("/api/v1/runs"), runs=data.runs;
  current=runs.find(r=>r.status==="running") || runs[0] || null;
  $("start-button").disabled=launchPending || Boolean(runs.find(r=>r.status==="running")) || !$("model").value;
  $("stop-button").disabled=!current || current.status!=="running";
  $("inspect-button").disabled=!current;
  if(current){
    $("job-id").textContent=current.run_id;
    $("job-status").replaceWith(Object.assign(pill(current.status),{id:"job-status"}));
    $("job-phase").textContent=phaseNames[current.phase] || current.phase;
    $("job-detail").textContent=current.error || ("已记录 "+(current.recorded_requests||0)+" / "+current.config.requests+" 次正式请求");
    $("job-progress").style.width=Math.min(100,(current.recorded_requests||0)/current.config.requests*100)+"%";
  }
  $("run-list").replaceChildren();
  if(!runs.length){const tr=document.createElement("tr"),td=document.createElement("td");td.colSpan=5;td.className="empty";td.textContent="暂无实验记录。从上方启动第一次实验。";tr.append(td);$("run-list").append(tr);}
  for(const run of runs){
    const tr=document.createElement("tr");
    for(const text of [run.run_id,run.config.policy]){const td=document.createElement("td");td.textContent=text;tr.append(td);}
    const status=document.createElement("td");status.append(pill(run.status));tr.append(status);
    const count=document.createElement("td");count.textContent=(run.recorded_requests||0)+" / "+run.config.requests;tr.append(count);
    const action=document.createElement("td"),button=document.createElement("button");button.className="secondary";button.textContent="查看详情 ↗";button.onclick=()=>inspect(run.run_id);action.append(button);tr.append(action);$("run-list").append(tr);
  }
}
async function inspect(id){
  try{
    const data=await api("/api/v1/runs/"+encodeURIComponent(id));selected=id;
    $("detail-id").textContent=id;$("artifacts").replaceChildren();$("detail-metrics").replaceChildren();
    for(const filename of data.artifacts){const a=document.createElement("a");a.textContent=filename;a.href="/api/v1/runs/"+encodeURIComponent(id)+"/files/"+filename;a.target="_blank";$("artifacts").append(a);}
    if(data.summary)for(const [label,key] of [["成功请求","successful_requests"],["TTFT P95 / ms","client_ttft_ms_p95"],["E2E P95 / ms","client_e2e_ms_p95"]]){
      const el=document.createElement("span"),n=data.summary[key];el.textContent=label+"："+(Number.isFinite(n)?Number(n.toFixed(2)):"—");$("detail-metrics").append(el);
    }
    $("detail-text").textContent=JSON.stringify(data.analysis || data.manifest,null,2);
    if(!$("detail").open)$("detail").showModal();
  }catch(e){showError(e.message);}
}
function showError(message){$("error").hidden=!message;$("error").textContent=message;}
$("policy").onchange=()=>{const enabled=$("policy").value==="cpu_lru";$("cpu-blocks").disabled=!enabled;$("cpu-blocks").value=enabled?72:0;};
$("run-form").onsubmit=async e=>{
  e.preventDefault();showError("");launchPending=true;$("start-button").disabled=true;$("launch-message").textContent="正在创建实验…";
  const config={model_path:$("model").value,workload_path:$("workload").value,policy:$("policy").value,
    requests:Number($("requests").value),concurrency:Number($("concurrency").value),warmup:Number($("warmup").value),
    max_tokens:Number($("max-tokens").value),gpu_kv_blocks:Number($("gpu-blocks").value),cpu_kv_blocks:Number($("cpu-blocks").value)};
  try{await api("/api/v1/runs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(config)});$("launch-message").textContent="已创建，正在启动模型。";}
  catch(error){showError(error.message);$("launch-message").textContent="启动失败";}
  finally{launchPending=false;await renderRuns().catch(e=>showError(e.message));}
};
$("stop-button").onclick=async()=>{
  if(!current)return;$("stop-button").disabled=true;
  try{await api("/api/v1/runs/"+current.run_id+"/stop",{method:"POST"});$("job-phase").textContent="正在停止并保存数据…";}
  catch(e){showError(e.message);}
};
$("inspect-button").onclick=()=>current&&inspect(current.run_id);
$("close-detail").onclick=()=>$("detail").close();
window.addEventListener("resize",drawCharts);
async function poll(){
  try{const data=await api("/api/v1/history");history=data.samples;renderLive();await renderRuns();}
  catch(e){$("connection").textContent="连接中断 · 自动重试";showError(e.message);}
  finally{setTimeout(poll,2000);}
}
async function init(){
  try{
    const env=await api("/api/v1/environment");
    for(const [id,values] of [["model",env.models],["workload",env.workloads]]){
      for(const v of values){const option=document.createElement("option");option.value=v;option.textContent=v;$(id).append(option);}
    }
    if(!env.models.length)showError("未找到本地模型。配置 TRACEKV_MODEL_ROOT 后重新启动平台。");
  }catch(e){showError(e.message);}
  poll();
}
init();
