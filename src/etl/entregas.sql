-- Databricks notebook source
with tb_pedido as (
  select 
     t1.idPedido,
     t2.idVendedor,
     t1.descSituacao,
     t1.dtPedido,
     t1.dtAprovado,
     t1.dtEntregue,
     t1.dtEstimativaEntrega,
     sum(vlFrete) as totalFrete
  from silver.olist.pedido as t1
  left join silver.olist.item_pedido as t2
    on t1.idPedido = t2.idPedido
  where t1.dtPedido < '2018-01-01' 
    and t1.dtPEdido >= add_months('2018-01-01', -6)
  group by 
    t1.idPedido,
    t2.idVendedor,
    t1.descSituacao,
    t1.dtPedido,
    t1.dtAprovado,
    t1.dtEntregue,
    t1.dtEstimativaEntrega
  ) 
  
  select idVendedor, 
    count(distinct case 
      when descSituacao = 'delivered' 
        and date(coalesce(dtEntregue,'2018-01-01')) > date(dtEstimativaEntrega) 
        then idPedido
    end) as pctPedidoAtraso,
    count(case when descSituacao = 'canceled' then idPedido end) / count(distinct idPedido) as pctPedidoCancelado,
    avg(totalFrete) as avgFrete,
    percentile(totalFrete, 0.5) as medianFrete,
    max(totalFrete) as maxFrete,
    min(totalFrete) as minFrete,
    avg(datediff(coalesce(dtEntregue,'2018-01-01'),dtAprovado)) AS qtdDiasAprovadoEntrega,
    avg(datediff(coalesce(dtEntregue,'2018-01-01'),dtPedido)) AS qtdDiasPedidoEntrega,
    avg(datediff(coalesce(dtEntregue,'2018-01-01'),dtEstimativaEntrega)) AS qtdDiasEntregaPromessa
    from tb_pedido
    group by 1
  


-- COMMAND ----------


