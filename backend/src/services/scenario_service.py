import csv,io
from fastapi import HTTPException
from ..models import BudgetScenario,AttributionRun,JourneyBatch
from ..utils.serialize import record
from ..utils.fingerprint import fingerprint
from .audit_service import log
from .budget_algorithm import optimize_budget
from .channel_service import list_channels
from .run_service import queue

EPS=1e-6

def list_scenarios(db):return [record(s) for s in db.query(BudgetScenario).order_by(BudgetScenario.id.desc())]
def get_scenario(db,sid):
    row=db.get(BudgetScenario,sid)
    if not row:raise HTTPException(404,"情景不存在")
    return row

def occupied_by_channel(db,batch_id):
    """同批次全部研究下已获批情景占用的渠道额度；待审、退回与已撤回均不占用。"""
    rows=(db.query(BudgetScenario).join(AttributionRun,BudgetScenario.run_id==AttributionRun.id)
          .filter(AttributionRun.batch_id==batch_id,BudgetScenario.review_status=="approved")
          .order_by(BudgetScenario.id).all())
    occupied={}
    for s in rows:
        for a in (s.result or {}).get('allocations',[]):occupied[a['code']]=occupied.get(a['code'],0.)+float(a['allocation'])
    return occupied,rows

def remaining_map(db,batch_id):
    occupied,_=occupied_by_channel(db,batch_id)
    return {c['code']:max(0.,c['capacity']-occupied.get(c['code'],0.)) for c in list_channels(db)}

def batch_quota(db,batch_id):
    if not db.get(JourneyBatch,batch_id):raise HTTPException(404,"路径批次不存在")
    occupied,holders=occupied_by_channel(db,batch_id)
    by_code={}
    for s in holders:
        for a in (s.result or {}).get('allocations',[]):
            if a['allocation']>EPS:by_code.setdefault(a['code'],[]).append({"scenario_id":s.id,"scenario":s.name,"allocation":a['allocation']})
    return [{"code":c['code'],"name":c['name'],"capacity":c['capacity'],"occupied":occupied.get(c['code'],0.),"available":max(0.,c['capacity']-occupied.get(c['code'],0.)),"holders":by_code.get(c['code'],[])} for c in list_channels(db)]

def _lock_batch(db,batch_id):
    """批次额度互斥锁：审批与撤回都先锁定批次行，并发操作串行核对。"""
    return db.query(JourneyBatch).filter(JourneyBatch.id==batch_id).with_for_update().first()
def _locked_scenario(db,sid):
    """行锁重读并强制刷新会话缓存，确保复核/撤回看到的是最新已提交状态。"""
    return db.query(BudgetScenario).filter(BudgetScenario.id==sid).with_for_update().populate_existing().first()

def create_scenario(db,data,user):
    run=db.get(AttributionRun,data.run_id)
    if not run or run.status!="completed":raise HTTPException(409,"必须选已完成归因研究")
    try:optimize_budget(run.result['credits'],data.budget,data.capacity_overrides)
    except ValueError as e:raise HTTPException(422,str(e))
    row=BudgetScenario(**data.model_dump(),owner=user.name)
    db.add(row);db.flush();log(db,user.email,"scenario.enqueue",row.id);db.commit()
    try:queue.rpush("journey:jobs","scenario:"+str(row.id))
    except Exception:
        row.status="failed";row.error="队列不可用，请重试";db.commit();raise HTTPException(503,row.error)
    return record(row)

def solve_scenario(db,row):
    """worker求解入口：按当前批次剩余额度分配，剩余额度随结果留痕。"""
    run=db.get(AttributionRun,row.run_id)
    codes={c['code'] for c in run.result['credits']}
    remaining={k:v for k,v in remaining_map(db,run.batch_id).items() if k in codes}
    result=optimize_budget(run.result['credits'],row.budget,row.capacity_overrides,remaining)
    result['input_hash']=fingerprint({"method":"linear-association-budget-v1","attribution":run.result,"budget":row.budget,"capacity_overrides":row.capacity_overrides,"remaining":remaining})
    result['attribution_hash']=run.result['input_hash'];row.result=result

def review_scenario(db,sid,data,user):
    row=get_scenario(db,sid)
    if row.status!="completed":raise HTTPException(409,"只能复核已完成情景")
    run=db.get(AttributionRun,row.run_id)
    if data.status=="approved":_lock_batch(db,run.batch_id)
    locked=_locked_scenario(db,sid)
    if locked.review_status!="submitted":raise HTTPException(409,"已复核情景不能重复审批")
    if data.status=="approved":
        remaining=remaining_map(db,run.batch_id)
        gaps=[]
        for a in locked.result.get('allocations',[]):
            need=float(a['allocation']);avail=remaining.get(a['code'],0.)
            if need>avail+EPS:gaps.append(f"{a['name']}（{a['code']}）需 {need:.3f}、批次剩余 {avail:.3f}、缺口 {need-avail:.3f}")
        if gaps:raise HTTPException(409,"批次渠道额度不足，批准冲突："+"；".join(gaps)+"。同批次各研究共享渠道额度，待审与退回不占用；可撤回其他获批情景释放额度后重试。")
    locked.review_status=data.status;locked.reviewer=user.name;locked.review_note=data.review_note
    log(db,user.email,"scenario."+data.status,locked.id);db.commit();return record(locked)

def withdraw_scenario(db,sid,user):
    row=get_scenario(db,sid)
    run=db.get(AttributionRun,row.run_id)
    _lock_batch(db,run.batch_id)
    locked=_locked_scenario(db,sid)
    if locked.review_status!="approved":raise HTTPException(409,"仅获批情景可撤回；当前状态不占用批次额度，不能重复撤回")
    locked.review_status="withdrawn"
    log(db,user.email,"scenario.withdrawn",locked.id);db.commit();return record(locked)

def export_scenario(db,sid):
    row=get_scenario(db,sid)
    if row.status!="completed" or row.review_status!="approved":raise HTTPException(403,"只有获批完成情景可以导出")
    out=io.StringIO();w=csv.writer(out);w.writerow(['channel','allocation','unit_cost','capacity','spend','association_score','proxy_contribution','attribution_share','input_hash'])
    for x in row.result['allocations']:w.writerow([x[k] for k in ['code','allocation','unit_cost','capacity','spend','association_score','proxy_contribution','share']]+[row.result['input_hash']])
    return out.getvalue()
