"""Version-2 bounded checkpoints, order interning and hierarchical deltas.

This transport knows record shapes, never economic arithmetic or decisions.
"""
from replay.preparation import encoded
from replay.strategy_sdk import LineWriter
from replay.streams.protocol import obj,require
from .contract import MAX_LINE,wire

CHECKPOINT_EVERY=1024
ORDER_FIELDS=frozenset('key quantity gross_quantity gross_cost fee_priced_ns fee_priced_sequence fee_priced_scope cash retained asset venue outcome_asset source taken ask_taken assessment_ids charges assumptions evidence market_id shape keys rule_identity price_scale quantity_scale quantity_increment_atoms'.split())
MODEL_FIELDS=frozenset('key asset venue outcome_asset source market_id shape keys rule_identity price_scale quantity_scale quantity_increment_atoms'.split())
PORTFOLIO_FIELDS=frozenset('orders margin cost floor gross_floor gross_margin holding_bound native_vectors classification'.split())


def pack(row):
    definitions={};models={};assets={};portfolio_definitions={};portfolios={}
    def visit(node,portfolio=True):
        if type(node)is dict:
            if portfolio and node.keys()==PORTFOLIO_FIELDS:
                identity=encoded(node);portfolio_definitions[identity]=node;portfolios[identity]=visit(node,False)
                return {'portfolio_ref':identity}
            if node.keys()==ORDER_FIELDS:
                identity=encoded(node)
                definitions[identity]=node
                return {'order_ref':identity}
            if node.keys()=={'kind','ledger','token'}:
                identity=encoded(node);assets[identity]=node;return {'asset_ref':identity}
            return {key:visit(node[key])for key in sorted(node)}
        if type(node)is list:return [visit(v)for v in node]
        return node
    row=visit(row)
    identities=sorted(definitions,key=lambda i:(definitions[i]['fee_priced_ns'],definitions[i]['key'],definitions[i]['quantity'],i))
    indices={identity:index for index,identity in enumerate(identities)}
    orders=[]
    for identity in identities:
        order=definitions[identity];model={key:order[key]for key in sorted(MODEL_FIELDS)}
        model_identity=encoded(model);models[model_identity]=visit(model)
        orders.append({'model_ref':model_identity,**{key:visit(order[key])for key in sorted(ORDER_FIELDS-MODEL_FIELDS)}})
    model_identities=sorted(models);asset_identities=sorted(assets)
    model_indices={identity:index for index,identity in enumerate(model_identities)}
    asset_indices={identity:index for index,identity in enumerate(asset_identities)}
    portfolio_identities=sorted(portfolios,key=lambda i:(portfolio_definitions[i]['orders'][0]['fee_priced_ns']if portfolio_definitions[i]['orders']else-1,[(o['key'],o['quantity'])for o in portfolio_definitions[i]['orders']],i))
    portfolio_indices={identity:index for index,identity in enumerate(portfolio_identities)}
    def bind(node):
        if type(node)is dict:
            if node.keys()=={'order_ref'}:return {'order_ref':indices[node['order_ref']]}
            if node.keys()=={'asset_ref'}:return {'asset_ref':asset_indices[node['asset_ref']]}
            if node.keys()=={'portfolio_ref'}:return {'portfolio_ref':portfolio_indices[node['portfolio_ref']]}
            return {key:model_indices[value]if key=='model_ref'else bind(value)for key,value in node.items()}
        if type(node)is list:return [bind(value)for value in node]
        return node
    return {'assets':[assets[i]for i in asset_identities],'models':[bind(models[i])for i in model_identities],'orders':bind(orders),'portfolios':[bind(portfolios[i])for i in portfolio_identities],'row':bind(row)}


def unpack(state):
    obj(state,'assets models orders portfolios row');require(type(state['row'])is dict and all(type(state[key])is list and len(state[key])<=100000 for key in('assets','models','orders','portfolios')),'bounded order pool')
    for asset in state['assets']:obj(asset,'kind ledger token')
    for model in state['models']:obj(model,' '.join(MODEL_FIELDS))
    for order in state['orders']:obj(order,' '.join((ORDER_FIELDS-MODEL_FIELDS)|{'model_ref'}))
    for portfolio in state['portfolios']:obj(portfolio,' '.join(PORTFOLIO_FIELDS))
    visited=0
    def visit(node,depth=0):
        nonlocal visited
        visited+=1;require(depth<=32 and visited<=2000000,'expanded decision node/depth bound')
        if type(node)is dict:
            if node.keys()=={'portfolio_ref'}:
                index=node['portfolio_ref'];require(type(index)is int and 0<=index<len(state['portfolios']),'existing portfolio reference')
                return visit(state['portfolios'][index],depth+1)
            if node.keys()=={'order_ref'}:
                index=node['order_ref'];require(type(index)is int and 0<=index<len(state['orders']),'existing order reference')
                order=state['orders'][index];model_index=order['model_ref']
                require(type(model_index)is int and 0<=model_index<len(state['models']),'existing native model reference')
                return {**visit(state['models'][model_index],depth+1),**{key:visit(value,depth+1)for key,value in order.items()if key!='model_ref'}}
            if node.keys()=={'asset_ref'}:
                index=node['asset_ref'];require(type(index)is int and 0<=index<len(state['assets']),'existing asset reference')
                return state['assets'][index]
            return {key:visit(value,depth+1)for key,value in node.items()}
        if type(node)is list:return [visit(value,depth+1)for value in node]
        return node
    row=visit(state['row'])
    require(pack(row)==state,'canonical unique sorted order pool')
    return row


def changes(previous,current):
    if previous==current:return None
    if type(current)is list and all(type(value)not in(dict,list)for value in current):return [0,current]
    if type(previous)is dict and type(current)is dict and previous.keys()==current.keys():
        patch=[1,*[part for index,key in enumerate(sorted(current))if (change:=changes(previous[key],current[key]))is not None for part in(index,change)]]
    elif type(previous)is list and type(current)is list:
        patch=[2,len(current),*[part for index,value in enumerate(current)if (change:=changes(previous[index],value)if index<len(previous)else[0,value])is not None for part in(index,change)]]
    else:return [0,current]
    replacement=[0,current]
    return replacement if len(encoded(replacement))<len(encoded(patch))else patch


class DecisionWriter:
    def __init__(self,path,**limits):
        self.writer=LineWriter(path,**limits);self.previous=None;self.count=0
    @property
    def stream(self):return self.writer.stream
    def append(self,row):
        row=pack(wire(row));require(len(encoded(row))+1<=MAX_LINE,'expanded decision state bound')
        if self.count%CHECKPOINT_EVERY==0 or row['row'].get('type')=='terminal':
            stored={'version':2,'index':self.count,'kind':'checkpoint','state':row}
        else:stored={'version':2,'index':self.count,'kind':'delta','patch':changes(self.previous,row)}
        self.writer.append(stored);self.previous=row;self.count+=1
    def finish(self):return self.writer.finish()


def expand(stored,previous,index):
    """Copy-on-write reconstruction; closed, ordered, depth-bounded patch tree."""
    require(type(stored)is dict and stored.get('version')==2 and type(stored.get('version'))is int,'decision transport version')
    require(stored.get('index')==index and type(stored.get('index'))is int,'dense decision transport index')
    if stored.get('kind')=='checkpoint':
        obj(stored,'version index kind state');require(type(stored['state'])is dict,'decision checkpoint state')
        result=stored['state']
    else:
        obj(stored,'version index kind patch')
        require(stored['kind']=='delta'and previous is not None and index%CHECKPOINT_EVERY!=0,'required decision checkpoint')
        nodes=0
        def apply(node,patch,depth=0):
            nonlocal nodes
            nodes+=1;require(nodes<=100000 and depth<=32,'decision delta node/depth bound')
            require(type(patch)is list and len(patch)>=2 and type(patch[0])is int,'closed decision patch')
            tag=patch[0]
            if tag==0:require(len(patch)==2,'replacement patch');return patch[1]
            if tag==1:
                require(len(patch)>=3 and len(patch)%2==1 and type(node)is dict,'decision dictionary patch')
                result=node.copy();keys=sorted(node);last=-1
                for offset,child in zip(patch[1::2],patch[2::2]):
                    require(type(offset)is int and last<offset<len(keys),'ordered existing dictionary index')
                    key=keys[offset];last=offset;result[key]=apply(node[key],child,depth+1)
                return result
            require(tag==2 and len(patch)%2==0 and type(node)is list,'decision list patch')
            length=patch[1]
            require(type(length)is int and 0<=length<=1000000,'bounded patched list')
            result=node[:length];result.extend([None]*max(0,length-len(result)));last=-1
            for offset,child in zip(patch[2::2],patch[3::2]):
                require(type(offset)is int and last<offset<len(result),'ordered existing list index')
                require(offset<len(node)or type(child)is list and len(child)==2 and child[0]==0,'appended list value')
                last=offset;result[offset]=apply(node[offset]if offset<len(node)else None,child,depth+1)
            return result
        result=apply(previous,stored['patch'])
        require(stored['patch']==changes(previous,result),'canonical decision delta')
    require(type(result)is dict and len(encoded(result))+1<=MAX_LINE,'expanded decision state bound')
    return result
