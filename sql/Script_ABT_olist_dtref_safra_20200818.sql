-- Scripts para criação da ABT
-- Autor: Sergio Zaccarelli
-- Data: Jun/2020

-- INICIO DAS PRE-ABTs
-- As tabelas abaixo precisam ser executadas ANTES, para criar agregações e transformações
-- Isso acelera o processo de criação as ABTs

DROP TABLE IF EXISTS olist_payments_preabt;  -- Comando DROP caso necessite recriar tabela
-- Tabela que sumariza em linha única todas infos de pgto por order_id
-- É necessária pois cada ordem pode ser paga com vários meios simultâneamente
CREATE TABLE olist_payments_preabt as
select
order_id,
count(payment_sequential) as qt_payments,
count(distinct payment_type) as qt_payment_types,
sum(case when payment_type = 'credit_card' then payment_installments else 0 end) as installments_credit_card,
sum(case when payment_type = 'credit_card' then payment_value else 0 end) as value_credit_card,
sum(case when payment_type = 'debit_card' then payment_installments else 0 end) as installments_debit_card,
sum(case when payment_type = 'debit_card' then payment_value else 0 end) as value_debit_card,
sum(case when payment_type = 'boleto' then payment_installments else 0 end) as installments_boleto,
sum(case when payment_type = 'boleto' then payment_value else 0 end) as value_boleto,
sum(case when payment_type = 'voucher' then payment_installments else 0 end) as installments_voucher,
sum(case when payment_type = 'voucher' then payment_value else 0 end) as value_voucher
from olist_order_payments_dataset
group by order_id;

DROP TABLE IF EXISTS olist_order_items_preabt; -- Comando DROP caso necessite recriar tabela
-- Tabela que agrupa para cada ordem os mesmos produtos em uma só linha
-- É necessária para sumarizar as quantidades e preço por produto
create table olist_order_items_preabt as
select
    seller_id,
    order_id,
    product_id,
    count(order_item_id) as qt_itens,
    sum(price) as value_price,
    sum(freight_value) as value_freight
from olist_order_items_dataset
group by
    order_id,
    product_id,
    seller_id;

DROP TABLE IF EXISTS olist_reviews_preabt; -- Comando DROP caso necessite recriar tabela
-- Tabela que agrupa para cada ordem os diversos reviews em uma só linha
-- É necessária pois é possível fazer vários reviews para a mesma ordem
create table olist_reviews_preabt as    
select 
order_id,
count(review_id) as qt_reviews,
AVG(review_score) as avg_score,
FIRST_VALUE(review_score) OVER (ORDER BY review_creation_date DESC) as last_score, --olhar pq não está certo
avg(julianday(date(review_answer_timestamp))-julianday(date(review_creation_date))) as avg_days_create_answer 
from olist_order_reviews_dataset
group by order_id;

DROP TABLE IF EXISTS olist_products_preabt; -- Comando DROP caso necessite recriar tabela
-- tabela que cria para cada seller_id as variáveis demandadas de produto da ABT
-- Ela será vinculada na última query que cria a ABT por já ter agrupamento por seller
CREATE TABLE olist_products_preabt as
select seller_id,
1.0*count(product_id)/count(distinct ifnull(product_category_name,'')) as quantidade_de_produtos_por_categoria,
1.0*sum(product_photos_qty)/count(product_id) as media_num_fotos_por_produto,
1.0*sum(product_photos_qty)/count(ifnull(product_category_name,'')) as media_fotos_por_produto_categoria,
1.0*sum(product_description_lenght)/count(product_id) as media_tamanho_descriçao_por_produto,
count(product_id) as qtd_produtos_vendendo,
count(distinct ifnull(product_category_name,'')) as qtd_categorias_vendendo from
(select distinct oit.seller_id,prd.* 
from olist_order_items_preabt oit
inner join olist_products_dataset prd
ON (oit.product_id = prd.product_id)) selprod
group by seller_id;

DROP TABLE IF EXISTS olist_flvendas_preabt; -- Comando DROP caso necessite recriar tabela
-- Tabela criada para identificar se o vendedor fez alguma venda no mês
-- OBS: no caso aqui a data referência é do mês SEGUINTE ao mês que se quer extrair o flag
-- Ela será vinculada na última query que cria a ABT por já ter agrupamento por seller
-- IMPORTANTE: rodar essa query sobre a olist original, com todos os meses
CREATE TABLE olist_flvendas_preabt as
select distinct seller_id,
1 as fl_venda
from olist_order_items_dataset
where order_id in
(select order_id from olist_orders_dataset --note o database name antes do nome da tabela, para rodar no DB original com todas as ordens
 where order_purchase_timestamp between date('{data_ref}') and date('{data_ref}','+1 month','-1 day'));

-- FIM PRE-ABTS


-- INICIO DAS VIEWS - ABT parte 1

--drop view vw_olist_abt_p1 -- comando DROP caso necessite recriar a view
--pragma table_info('vw_olist_abt_p1') --caso queira ver as colunas da view

-- view que faz a primeira camada de agregações e já prepara a maioria das variáveis da ABT final
-- seguem comentários dentro da própria consulta dos trechos mais relevantes
-- OBS: sugiro ler para entender de baixo para cima
-- OBS2: algumas colunas não são variáveis da ABT, estão aí para checagem e outras serão usadas nos cálculos da view p2 seguinte a esta
-- Explicações sobre algumas funções novas utilizadas
-- julianday() - transforma a data numa data juliana, em dias (estilo Excel), com isso podemos subtrair duas datas para obter os dias entre elas
-- date() - serve para somar ou subtrair dias e meses de datas. Usei para criar os ranges para os group bys e wheres
-- substr() - extrai uma porção de uma string, basicamente usei para tirar o ano e o mês da data, para o calculo de meses entre duas datas
DROP VIEW IF EXISTS vw_olist_abt_p1; -- Comando DROP caso necessite recriar tabela
create view vw_olist_abt_p1 as
SELECT 
    orditems.seller_id,

    min(order_purchase_yearmonth) as min_purchase_yearmonth,
    min(order_purchase_date) as min_purchase_date,

    refdate_julianday-julianday(max(date(refdate,'-6 months'),min(order_purchase_date))) as qt_total_dias_considerado, -- dias a considerar para ativação, 6 meses atrás ou o primeiro dia de venda, o que for maior
    count(distinct case when order_purchase_date >=date(refdate,'-6 months') then order_purchase_date else null end ) as qt_dias_venda, 
 

    max(date(refdate,'-6 months'),min(order_purchase_date)) as min_purchase_date_considerada, 

    -- nota: para a diferença de meses entre a data ref e o primeiro dia de venda, transformei o ano+mês no total de meses, e subtrai entre as duas datas
    min((refdate_year*12+refdate_month)-(substr(min(order_purchase_yearmonth),1,4)*12+substr(min(order_purchase_yearmonth),5,2)),6) as qt_total_meses_considerado, -- meses a considerar para ativação, 6 meses atrás ou o primeiro mês de venda, o que for maior
    min(count(distinct order_purchase_yearmonth),6) as qt_meses_venda_considerado,
    MAX(orders.order_purchase_julianday) max_purchase_date,
    MAX(orders.order_delivered_customer_julianday) AS max_delivered_customer_date,
    refdate_julianday-MAX(orders.order_purchase_julianday) as dias_desde_ult_venda,
    refdate_julianday-MAX(orders.order_estimated_delivery_julianday) as dias_desde_ult_estimativa_entrega,
    refdate_julianday-MAX(orders.order_delivered_customer_julianday) as dias_desde_ult_entrega_pedido,

    count(distinct orders.order_id) as qtd_pedidos_ate_hoje,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day')
                then orders.order_id else null end)) as qtd_pedidos_ultimo_mes,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day')
                then orders.order_id else null end)) as qtd_pedidos_ultimos_2_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day')
                then orders.order_id else null end)) as qtd_pedidos_ultimos_3_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day')
                then orders.order_id else null end)) as qtd_pedidos_ultimos_4_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day')
                then orders.order_id else null end)) as qtd_pedidos_ultimos_5_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day')
                then orders.order_id else null end)) as qtd_pedidos_ultimos_6_meses,

    count(distinct (case when order_status='delivered' and order_delivered_customer_julianday is not null -- pedido entregue e que não seja em data futura (olhe a subquery onde "anulo" entregas lá embaixo)
               then orders.order_id else null end)) as qtd_pedidos_entregues_ate_hoje,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day')
                    and order_status='delivered' and order_delivered_customer_julianday is not null 
               then orders.order_id else null end)) as qtd_pedidos_entregues_ultimo_mes,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day')
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then orders.order_id else null end)) as qtd_pedidos_entregues_ultimos_2_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day')
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then orders.order_id else null end)) as qtd_pedidos_entregues_ultimos_3_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day')
                    and order_status='delivered' and order_delivered_customer_julianday is not null 
               then orders.order_id else null end)) as qtd_pedidos_entregues_ultimos_4_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day')
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then orders.order_id else null end)) as qtd_pedidos_entregues_ultimos_5_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day')
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then orders.order_id else null end)) as qtd_pedidos_entregues_ultimos_6_meses,

    count(distinct (case when order_status='canceled' 
               then orders.order_id else null end)) as qtd_pedidos_cancelados_ate_hoje,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day')
                    and order_status='canceled' 
               then orders.order_id else null end)) as qtd_pedidos_cancelados_ultimo_mes,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day')
                    and order_status='canceled'
                then orders.order_id else null end)) as qtd_pedidos_cancelados_ultimos_2_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day')
                    and order_status='canceled'
                then orders.order_id else null end)) as qtd_pedidos_cancelados_ultimos_3_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day')
                    and order_status='canceled' 
               then orders.order_id else null end)) as qtd_pedidos_cancelados_ultimos_4_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day')
                    and order_status='canceled'
                then orders.order_id else null end)) as qtd_pedidos_cancelados_ultimos_5_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day')
                    and order_status='canceled'
                then orders.order_id else null end)) as qtd_pedidos_cancelados_ultimos_6_meses,

    count(distinct (case when order_status not in ('delivered','canceled') 
               then orders.order_id else null end)) as qtd_pedidos_outros_status_ate_hoje,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day')
                    and order_status not in ('delivered','canceled') 
               then orders.order_id else null end)) as qtd_pedidos_outros_status_ultimo_mes,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day')
                    and order_status not in ('delivered','canceled')
                then orders.order_id else null end)) as qtd_pedidos_outros_status_ultimos_2_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day')
                    and order_status not in ('delivered','canceled')
                then orders.order_id else null end)) as qtd_pedidos_outros_status_ultimos_3_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day')
                    and order_status not in ('delivered','canceled') 
               then orders.order_id else null end)) as qtd_pedidos_outros_status_ultimos_4_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day')
                    and order_status not in ('delivered','canceled')
                then orders.order_id else null end)) as qtd_pedidos_outros_status_ultimos_5_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day')
                    and order_status not in ('delivered','canceled')
                then orders.order_id else null end)) as qtd_pedidos_outros_status_ultimos_6_meses,

    count(distinct (case when order_status = 'delivered' and order_estimated_delivery_julianday < order_delivered_customer_julianday 
               then orders.order_id else null end)) as qtd_pedidos_dt_prevista_menor_data_entrega_ate_hoje,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day')
                    and order_status = 'delivered' and order_estimated_delivery_julianday < order_delivered_customer_julianday 
                then orders.order_id else null end)) as qtd_pedidos_dt_prevista_menor_data_entrega_ultimo_mes,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day')
                    and order_status = 'delivered' and order_estimated_delivery_julianday < order_delivered_customer_julianday 
                then orders.order_id else null end)) as qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_2_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day')
                    and order_status = 'delivered' and order_estimated_delivery_julianday < order_delivered_customer_julianday 
                then orders.order_id else null end)) as qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_3_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day')
                    and order_status = 'delivered' and order_estimated_delivery_julianday < order_delivered_customer_julianday 
                then orders.order_id else null end)) as qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_4_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day')
                    and order_status = 'delivered' and order_estimated_delivery_julianday < order_delivered_customer_julianday 
                then orders.order_id else null end)) as qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_5_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day')
                    and order_status = 'delivered' and order_estimated_delivery_julianday < order_delivered_customer_julianday 
                then orders.order_id else null end)) as qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_6_meses,

    count( orditems.product_id ) as qtd_produtos_ate_hoje,
    count( (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day')
                then orditems.product_id else null end)) as qtd_produtos_ultimo_mes,
    count( (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day')
                then orditems.product_id else null end)) as qtd_produtos_ultimos_2_meses,
    count( (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day')
                then orditems.product_id else null end)) as qtd_produtos_ultimos_3_meses,
    count( (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day')
                then orditems.product_id else null end)) as qtd_produtos_ultimos_4_meses,
    count( (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day')
                then orditems.product_id else null end)) as qtd_produtos_ultimos_5_meses,
    count( (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day')
                then orditems.product_id else null end)) as qtd_produtos_ultimos_6_meses,

    avg(order_estimated_delivery_julianday-order_purchase_julianday) as media_dias_entre_dt_compra_dt_estim_entrega_ate_hoje,
    avg(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_estim_entrega_ultimo_mes,
    avg(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_estim_entrega_ultimos_2_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_estim_entrega_ultimos_3_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_estim_entrega_ultimos_4_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_estim_entrega_ultimos_5_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_estim_entrega_ultimos_6_meses,

    avg(order_delivered_customer_julianday-order_purchase_julianday ) as media_dias_entre_dt_compra_dt_entrega_ate_hoje,
    avg(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                then order_delivered_customer_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_entrega_ultimo_mes,
    avg(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                then order_delivered_customer_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_entrega_ultimos_2_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                then order_delivered_customer_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_entrega_ultimos_3_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                then order_delivered_customer_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_entrega_ultimos_4_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                then order_delivered_customer_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_entrega_ultimos_5_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                then order_delivered_customer_julianday-order_purchase_julianday 
                    else null end) as media_dias_entre_dt_compra_dt_entrega_ultimos_6_meses,

    avg(order_estimated_delivery_julianday-order_delivered_customer_julianday) as media_dias_entre_dt_estimada_dt_entrega_ate_hoje, --OBS: essa variável pode dar negativo
    avg(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_delivered_customer_julianday 
                    else null end) as media_dias_entre_dt_estimada_dt_entrega_ultimo_mes,
    avg(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_delivered_customer_julianday 
                    else null end) as media_dias_entre_dt_estimada_dt_entrega_ultimos_2_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_delivered_customer_julianday 
                    else null end) as media_dias_entre_dt_estimada_dt_entrega_ultimos_3_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_delivered_customer_julianday 
                    else null end) as media_dias_entre_dt_estimada_dt_entrega_ultimos_4_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_delivered_customer_julianday 
                    else null end) as media_dias_entre_dt_estimada_dt_entrega_ultimos_5_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                then order_estimated_delivery_julianday-order_delivered_customer_julianday 
                    else null end) as media_dias_entre_dt_estimada_dt_entrega_ultimos_6_meses,

    sum(value_price) as total_vlr_pedido_ate_hoje,
    sum(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                then value_price
                    else null end) as total_vlr_pedido_ultimo_mes,
    sum(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                then value_price
                    else null end) as total_vlr_pedido_ultimos_2_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                then value_price
                    else null end) as total_vlr_pedido_ultimos_3_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                then value_price
                    else null end) as total_vlr_pedido_ultimos_4_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                then value_price
                    else null end) as total_vlr_pedido_ultimos_5_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                then value_price
                    else null end) as total_vlr_pedido_ultimos_6_meses,

    sum(value_freight) as total_vlr_frete_ate_hoje,
    sum(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                then value_freight
                    else null end) as total_vlr_frete_ultimo_mes,
    sum(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                then value_freight
                    else null end) as total_vlr_frete_ultimos_2_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                then value_freight
                    else null end) as total_vlr_frete_ultimos_3_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                then value_freight
                    else null end) as total_vlr_frete_ultimos_4_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                then value_freight
                    else null end) as total_vlr_frete_ultimos_5_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                then value_freight
                    else null end) as total_vlr_frete_ultimos_6_meses,

    sum(case when order_status='delivered' and order_delivered_customer_julianday is not null -- pedido entregue e que não seja em data futura (olhe a subquery onde "anulo" entregas lá embaixo)
                then value_price
                    else null end) as total_vlr_pedidos_entregues_ate_hoje,
    sum(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then value_price
                    else null end) as total_vlr_pedidos_entregues_ultimo_mes,
    sum(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then value_price
                    else null end) as total_vlr_pedidos_entregues_ultimos_2_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then value_price
                    else null end) as total_vlr_pedidos_entregues_ultimos_3_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then value_price
                    else null end) as total_vlr_pedidos_entregues_ultimos_4_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then value_price
                    else null end) as total_vlr_pedidos_entregues_ultimos_5_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                    and order_status='delivered' and order_delivered_customer_julianday is not null
                then value_price
                    else null end) as total_vlr_pedidos_entregues_ultimos_6_meses,

    sum(case when order_status='canceled' -- pedido status cancelado
                then value_price
                    else null end) as total_vlr_pedidos_cancelados_ate_hoje,
    sum(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                    and order_status='canceled'
                then value_price
                    else null end) as total_vlr_pedidos_cancelados_ultimo_mes,
    sum(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                    and order_status='canceled'
                then value_price
                    else null end) as total_vlr_pedidos_cancelados_ultimos_2_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                    and order_status='canceled'
                then value_price
                    else null end) as total_vlr_pedidos_cancelados_ultimos_3_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                    and order_status='canceled'
                then value_price
                    else null end) as total_vlr_pedidos_cancelados_ultimos_4_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                    and order_status='canceled'
                then value_price
                    else null end) as total_vlr_pedidos_cancelados_ultimos_5_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                    and order_status='canceled'
                then value_price
                    else null end) as total_vlr_pedidos_cancelados_ultimos_6_meses,

    count(distinct (case when payments.installments_credit_card<>0 --pagamento com cartão de crédito
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_cc_ate_hoje,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_cc_ultimo_mes,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_cc_ultimos_2_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_cc_ultimos_3_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_cc_ultimos_4_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_cc_ultimos_5_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_cc_ultimos_6_meses,

    count(distinct (case when payments.installments_boleto<>0  -- pagamento com boleto
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_boleto_ate_hoje,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_boleto_ultimo_mes,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_boleto_ultimos_2_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_boleto_ultimos_3_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_boleto_ultimos_4_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_boleto_ultimos_5_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_boleto_ultimos_6_meses,

    count(distinct (case when payments.installments_debit_card+payments.installments_voucher<>0  -- pagamento com débito e/ou voucher
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_outros_ate_hoje,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_outros_ultimo_mes,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_outros_ultimos_2_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_outros_ultimos_3_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_outros_ultimos_4_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_outros_ultimos_5_meses,
    count(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then orders.order_id else NULL end)) as qtd_pedidos_pgto_outros_ultimos_6_meses,

    avg(distinct (case when payments.installments_credit_card<>0 -- se houve pagamento com cartão
            then payments.installments_credit_card else NULL end)) as media_qtd_parcelas_pedidos_pgto_cc_ate_hoje,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as media_qtd_parcelas_pedidos_pgto_cc_ultimo_mes,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as media_qtd_parcelas_pedidos_pgto_cc_ultimos_2_meses,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as media_qtd_parcelas_pedidos_pgto_cc_ultimos_3_meses,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as media_qtd_parcelas_pedidos_pgto_cc_ultimos_4_meses,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as media_qtd_parcelas_pedidos_pgto_cc_ultimos_5_meses,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as media_qtd_parcelas_pedidos_pgto_cc_ultimos_6_meses,

    avg(distinct (case when payments.installments_voucher<>0  --se houve pagamento com voucher 
            then payments.installments_voucher else NULL end)) as media_qtd_parcelas_pedidos_pgto_voucher_ate_hoje,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as media_qtd_parcelas_pedidos_pgto_voucher_ultimo_mes,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as media_qtd_parcelas_pedidos_pgto_voucher_ultimos_2_meses,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as media_qtd_parcelas_pedidos_pgto_voucher_ultimos_3_meses,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as media_qtd_parcelas_pedidos_pgto_voucher_ultimos_4_meses,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as media_qtd_parcelas_pedidos_pgto_voucher_ultimos_5_meses,
    avg(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as media_qtd_parcelas_pedidos_pgto_voucher_ultimos_6_meses,

    max(distinct (case when payments.installments_credit_card<>0  -- se houve pagamento com cartão de crédito
            then payments.installments_credit_card else NULL end)) as max_qtd_parcelas_pedidos_pgto_cc_ate_hoje,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as max_qtd_parcelas_pedidos_pgto_cc_ultimo_mes,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as max_qtd_parcelas_pedidos_pgto_cc_ultimos_2_meses,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as max_qtd_parcelas_pedidos_pgto_cc_ultimos_3_meses,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as max_qtd_parcelas_pedidos_pgto_cc_ultimos_4_meses,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as max_qtd_parcelas_pedidos_pgto_cc_ultimos_5_meses,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.installments_credit_card else NULL end)) as max_qtd_parcelas_pedidos_pgto_cc_ultimos_6_meses,

    max(distinct (case when payments.installments_voucher<>0 -- se houve pagamento com voucher 
            then payments.installments_voucher else NULL end)) as max_qtd_parcelas_pedidos_pgto_voucher_ate_hoje,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as max_qtd_parcelas_pedidos_pgto_voucher_ultimo_mes,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as max_qtd_parcelas_pedidos_pgto_voucher_ultimos_2_meses,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as max_qtd_parcelas_pedidos_pgto_voucher_ultimos_3_meses,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as max_qtd_parcelas_pedidos_pgto_voucher_ultimos_4_meses,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as max_qtd_parcelas_pedidos_pgto_voucher_ultimos_5_meses,
    max(distinct (case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_voucher<>0 
            then payments.installments_voucher else NULL end)) as max_qtd_parcelas_pedidos_pgto_voucher_ultimos_6_meses,

    sum(case when payments.installments_credit_card<>0 -- se houve pagamento com cartão de crédito
            then payments.value_credit_card else NULL end) as total_valor_pedidos_pgto_cc_ate_hoje,
    sum(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.value_credit_card else NULL end) as total_valor_pedidos_pgto_cc_ultimo_mes,
    sum(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.value_credit_card else NULL end) as total_valor_pedidos_pgto_cc_ultimos_2_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.value_credit_card else NULL end) as total_valor_pedidos_pgto_cc_ultimos_3_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.value_credit_card else NULL end) as total_valor_pedidos_pgto_cc_ultimos_4_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.value_credit_card else NULL end) as total_valor_pedidos_pgto_cc_ultimos_5_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_credit_card<>0 
            then payments.value_credit_card else NULL end) as total_valor_pedidos_pgto_cc_ultimos_6_meses,

    sum(case when payments.installments_boleto<>0 -- se houve pagamento com boleto
            then payments.value_boleto else NULL end) as total_valor_pedidos_pgto_boleto_ate_hoje,
    sum(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then payments.value_boleto else NULL end) as total_valor_pedidos_pgto_boleto_ultimo_mes,
    sum(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then payments.value_boleto else NULL end) as total_valor_pedidos_pgto_boleto_ultimos_2_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then payments.value_boleto else NULL end) as total_valor_pedidos_pgto_boleto_ultimos_3_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then payments.value_boleto else NULL end) as total_valor_pedidos_pgto_boleto_ultimos_4_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then payments.value_boleto else NULL end) as total_valor_pedidos_pgto_boleto_ultimos_5_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_boleto<>0 
            then payments.value_boleto else NULL end) as total_valor_pedidos_pgto_boleto_ultimos_6_meses,

    sum(case when payments.installments_debit_card+payments.installments_voucher<>0 -- se houver algum pagamento com voucher ou debito
            then payments.value_voucher+payments.value_debit_card else NULL end) as total_valor_pedidos_pgto_outros_ate_hoje,
    sum(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then payments.value_voucher+payments.value_debit_card else NULL end) as total_valor_pedidos_pgto_outros_ultimo_mes,
    sum(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then payments.value_voucher+payments.value_debit_card else NULL end) as total_valor_pedidos_pgto_outros_ultimos_2_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then payments.value_voucher+payments.value_debit_card else NULL end) as total_valor_pedidos_pgto_outros_ultimos_3_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then payments.value_voucher+payments.value_debit_card else NULL end) as total_valor_pedidos_pgto_outros_ultimos_4_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then payments.value_voucher+payments.value_debit_card else NULL end) as total_valor_pedidos_pgto_outros_ultimos_5_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
            and payments.installments_debit_card+payments.installments_voucher<>0 
            then payments.value_voucher+payments.value_debit_card else NULL end) as total_valor_pedidos_pgto_outros_ultimos_6_meses,
            
    sum(qt_reviews) as qtd_review_ate_hoje,
    sum(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                then qt_reviews
                    else null end) as qtd_review_ultimo_mes,
    sum(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                then qt_reviews
                    else null end) as qtd_review_ultimos_2_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                then qt_reviews
                    else null end) as qtd_review_ultimos_3_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                then qt_reviews
                    else null end) as qtd_review_ultimos_4_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                then qt_reviews
                    else null end) as qtd_review_ultimos_5_meses,
    sum(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                then qt_reviews
                    else null end) as qtd_review_ultimos_6_meses,

    avg(avg_score) as media_notas_review_ate_hoje,
    avg(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                then avg_score
                    else null end) as media_notas_review_ultimo_mes,
    avg(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                then avg_score
                    else null end) as media_notas_review_ultimos_2_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                then avg_score
                    else null end) as media_notas_review_ultimos_3_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                then avg_score
                    else null end) as media_notas_review_ultimos_4_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                then avg_score
                    else null end) as media_notas_review_ultimos_5_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                then avg_score
                    else null end) as media_notas_review_ultimos_6_meses,

    avg(avg_days_create_answer) as media_dias_entre_criacao_resposta_review_ate_hoje,
    avg(case when order_purchase_timestamp between date(refdate,'-1 months') and date(refdate,'-1 day') 
                then avg_days_create_answer
                    else null end) as media_dias_entre_criacao_resposta_review_ultimo_mes,
    avg(case when order_purchase_timestamp between date(refdate,'-2 months') and date(refdate,'-1 day') 
                then avg_days_create_answer
                    else null end) as media_dias_entre_criacao_resposta_review_ultimos_2_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-3 months') and date(refdate,'-1 day') 
                then avg_days_create_answer
                    else null end) as media_dias_entre_criacao_resposta_review_ultimos_3_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-4 months') and date(refdate,'-1 day') 
                then avg_days_create_answer
                    else null end) as media_dias_entre_criacao_resposta_review_ultimos_4_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-5 months') and date(refdate,'-1 day') 
                then avg_days_create_answer
                    else null end) as media_dias_entre_criacao_resposta_review_ultimos_5_meses,
    avg(case when order_purchase_timestamp between date(refdate,'-6 months') and date(refdate,'-1 day') 
                then avg_days_create_answer
                    else null end) as media_dias_entre_criacao_resposta_review_ultimos_6_meses
    FROM
(select 
    order_id,
    order_status,
    order_purchase_timestamp,
    date('{data_ref}') as refdate, -- criei essas colunas constantes para facilitar o replace de data, é só trocar aqui e sucesso, bem como as demais
    julianday('{data_ref}') as refdate_julianday,
    strftime('%Y%m','{data_ref}') as refdate_yearmonth,
    strftime('%Y','{data_ref}') as refdate_year,
    strftime('%m','{data_ref}') as refdate_month,
    date(order_purchase_timestamp) as order_purchase_date,
    strftime('%Y%m',order_purchase_timestamp) as order_purchase_yearmonth,
    julianday(date(order_purchase_timestamp)) as order_purchase_julianday,
    CASE WHEN (order_approved_at >= '{data_ref}') -- Case para "anular" valores futuros, com base em nossa data referência, para não aparecer algo entregue no futuro da data_ref
         THEN NULL 
         ELSE julianday(date(order_approved_at)) end AS order_approved_at_julianday,
    CASE WHEN (order_delivered_carrier_date >= '{data_ref}') 
         THEN NULL 
         ELSE julianday(date(order_delivered_carrier_date)) end AS order_delivered_carrier_julianday,
    CASE WHEN (order_delivered_customer_date >= '{data_ref}') 
         THEN NULL 
         ELSE julianday(date(order_delivered_customer_date)) end AS order_delivered_customer_julianday,
    julianday(date(order_estimated_delivery_date)) AS order_estimated_delivery_julianday
from olist_orders_dataset
where order_purchase_timestamp between date('{data_ref}','-12 months') and date('{data_ref}','-1 day')) as orders -- subquery que prepara e filtra ordens por data
INNER JOIN olist_order_items_preabt as orditems -- note que os JOINs são com as preabts
ON (orders.order_id = orditems.order_id)
INNER JOIN olist_payments_preabt as payments
ON (orders.order_id = payments.order_id)
LEFT JOIN olist_reviews_preabt as reviews
ON (orders.order_id = reviews.order_id)
group by orditems.seller_id;

-- View ABT parte 2

--DROP view vw_olist_abt_p2 -- comando DROP caso necessite recriar a view

-- View que propaga a p1 da ABT, e cria as variáveis faltantes para a ABT definitiva
-- a criação de uma segunda camada de view foi para facilitar o entendimento dos cálculos e
-- para deixar o processamento final mais clean
-- Note que aqui trouxemos JOINs com pre-abts criadas com visão já por seller
DROP VIEW IF EXISTS vw_olist_abt_p2;
create view vw_olist_abt_p2 as 
select 
sel.seller_id,

case when flv.fl_venda is null then 0 else 1 end as fl_venda,
sel.seller_zip_code_prefix,
sel.seller_city,
sel.seller_state,

-- a maioria dos campos que são calculados aqui são médias, que até poderiam ser calculadas na p1 
-- mas deixariam a view poluída, dificultando entendimento e manutenção
-- É preciso multiplicar por 1.0 para vir um resultado não-inteiro (com decimais)
(1.0*qt_meses_venda_considerado)/qt_total_meses_considerado as proporcao_ativacao_meses,
(1.0*qt_dias_venda)/qt_total_dias_considerado as proporcao_ativacao_dias,

dias_desde_ult_venda,
dias_desde_ult_estimativa_entrega,
dias_desde_ult_entrega_pedido,

qtd_pedidos_ate_hoje,
qtd_pedidos_ultimo_mes,
qtd_pedidos_ultimos_2_meses,
qtd_pedidos_ultimos_3_meses,
qtd_pedidos_ultimos_4_meses,
qtd_pedidos_ultimos_5_meses,
qtd_pedidos_ultimos_6_meses,

(1.0*qtd_pedidos_ultimo_mes)/qtd_pedidos_ultimos_3_meses as tend_pedidos_m1_m3,
(1.0*qtd_pedidos_ultimo_mes)/qtd_pedidos_ultimos_6_meses as tend_pedidos_m1_m6,
(1.0*qtd_pedidos_ultimos_3_meses)/qtd_pedidos_ultimos_6_meses as tend_pedidos_m3_m6,

qtd_produtos_ate_hoje,
qtd_produtos_ultimo_mes,
qtd_produtos_ultimos_2_meses,
qtd_produtos_ultimos_3_meses,
qtd_produtos_ultimos_4_meses,
qtd_produtos_ultimos_5_meses,
qtd_produtos_ultimos_6_meses,

media_dias_entre_dt_compra_dt_estim_entrega_ate_hoje,
media_dias_entre_dt_compra_dt_estim_entrega_ultimo_mes,
media_dias_entre_dt_compra_dt_estim_entrega_ultimos_2_meses,
media_dias_entre_dt_compra_dt_estim_entrega_ultimos_3_meses,
media_dias_entre_dt_compra_dt_estim_entrega_ultimos_4_meses,
media_dias_entre_dt_compra_dt_estim_entrega_ultimos_5_meses,
media_dias_entre_dt_compra_dt_estim_entrega_ultimos_6_meses,

media_dias_entre_dt_compra_dt_entrega_ate_hoje,
media_dias_entre_dt_compra_dt_entrega_ultimo_mes,
media_dias_entre_dt_compra_dt_entrega_ultimos_2_meses,
media_dias_entre_dt_compra_dt_entrega_ultimos_3_meses,
media_dias_entre_dt_compra_dt_entrega_ultimos_4_meses,
media_dias_entre_dt_compra_dt_entrega_ultimos_5_meses,
media_dias_entre_dt_compra_dt_entrega_ultimos_6_meses,

media_dias_entre_dt_estimada_dt_entrega_ate_hoje,
media_dias_entre_dt_estimada_dt_entrega_ultimo_mes,
media_dias_entre_dt_estimada_dt_entrega_ultimos_2_meses,
media_dias_entre_dt_estimada_dt_entrega_ultimos_3_meses,
media_dias_entre_dt_estimada_dt_entrega_ultimos_4_meses,
media_dias_entre_dt_estimada_dt_entrega_ultimos_5_meses,
media_dias_entre_dt_estimada_dt_entrega_ultimos_6_meses,

total_vlr_pedido_ultimo_mes/qtd_pedidos_ultimo_mes as media_vlr_pedido_ultimo_mes,
total_vlr_pedido_ultimos_2_meses/qtd_pedidos_ultimos_2_meses as media_vlr_pedido_ultimos_2_meses,
total_vlr_pedido_ultimos_3_meses/qtd_pedidos_ultimos_3_meses as media_vlr_pedido_ultimos_3_meses,
total_vlr_pedido_ultimos_4_meses/qtd_pedidos_ultimos_4_meses as media_vlr_pedido_ultimos_4_meses,
total_vlr_pedido_ultimos_5_meses/qtd_pedidos_ultimos_5_meses as media_vlr_pedido_ultimos_5_meses,
total_vlr_pedido_ultimos_6_meses/qtd_pedidos_ultimos_6_meses as media_vlr_pedido_ultimos_6_meses,

1.0*(total_vlr_pedido_ultimo_mes/qtd_pedidos_ultimo_mes)/(total_vlr_pedido_ultimos_3_meses/qtd_pedidos_ultimos_3_meses) as tend_media_vlr_pedido_m1_m3,
1.0*(total_vlr_pedido_ultimo_mes/qtd_pedidos_ultimo_mes)/(total_vlr_pedido_ultimos_6_meses/qtd_pedidos_ultimos_6_meses) as tend_media_vlr_pedido_m1_m6,
1.0*(total_vlr_pedido_ultimos_3_meses/qtd_pedidos_ultimos_3_meses)/(total_vlr_pedido_ultimos_6_meses/qtd_pedidos_ultimos_6_meses) as tend_media_vlr_pedido_m3_m6,

total_vlr_frete_ultimo_mes/qtd_pedidos_ultimo_mes as media_vlr_frete_ultimo_mes,
total_vlr_frete_ultimos_2_meses/qtd_pedidos_ultimos_2_meses as media_vlr_frete_ultimos_2_meses,
total_vlr_frete_ultimos_3_meses/qtd_pedidos_ultimos_3_meses as media_vlr_frete_ultimos_3_meses,
total_vlr_frete_ultimos_4_meses/qtd_pedidos_ultimos_4_meses as media_vlr_frete_ultimos_4_meses,
total_vlr_frete_ultimos_5_meses/qtd_pedidos_ultimos_5_meses as media_vlr_frete_ultimos_5_meses,
total_vlr_frete_ultimos_6_meses/qtd_pedidos_ultimos_6_meses as media_vlr_frete_ultimos_6_meses,

total_vlr_frete_ultimo_mes/total_vlr_pedido_ultimo_mes as media_do_perc_valor_frete_contra_produto_ultimo_mes,
total_vlr_frete_ultimos_2_meses/total_vlr_pedido_ultimos_2_meses as media_do_perc_valor_frete_contra_produto_ultimos_2_meses,
total_vlr_frete_ultimos_3_meses/total_vlr_pedido_ultimos_3_meses as media_do_perc_valor_frete_contra_produto_ultimos_3_meses,
total_vlr_frete_ultimos_4_meses/total_vlr_pedido_ultimos_4_meses as media_do_perc_valor_frete_contra_produto_ultimos_4_meses,
total_vlr_frete_ultimos_5_meses/total_vlr_pedido_ultimos_5_meses as media_do_perc_valor_frete_contra_produto_ultimos_5_meses,
total_vlr_frete_ultimos_6_meses/total_vlr_pedido_ultimos_6_meses as media_do_perc_valor_frete_contra_produto_ultimos_6_meses,

qtd_pedidos_entregues_ate_hoje,
qtd_pedidos_entregues_ultimo_mes,
qtd_pedidos_entregues_ultimos_2_meses,
qtd_pedidos_entregues_ultimos_3_meses,
qtd_pedidos_entregues_ultimos_4_meses,
qtd_pedidos_entregues_ultimos_5_meses,
qtd_pedidos_entregues_ultimos_6_meses,

qtd_pedidos_cancelados_ate_hoje,
qtd_pedidos_cancelados_ultimo_mes,
qtd_pedidos_cancelados_ultimos_2_meses,
qtd_pedidos_cancelados_ultimos_3_meses,
qtd_pedidos_cancelados_ultimos_4_meses,
qtd_pedidos_cancelados_ultimos_5_meses,
qtd_pedidos_cancelados_ultimos_6_meses,

qtd_pedidos_outros_status_ate_hoje,
qtd_pedidos_outros_status_ultimo_mes,
qtd_pedidos_outros_status_ultimos_2_meses,
qtd_pedidos_outros_status_ultimos_3_meses,
qtd_pedidos_outros_status_ultimos_4_meses,
qtd_pedidos_outros_status_ultimos_5_meses,
qtd_pedidos_outros_status_ultimos_6_meses,

total_vlr_pedidos_entregues_ultimo_mes/qtd_pedidos_entregues_ultimo_mes as media_valores_pedidos_entregues_ultimo_mes,
total_vlr_pedidos_entregues_ultimos_2_meses/qtd_pedidos_entregues_ultimos_2_meses as media_valores_pedidos_entregues_ultimos_2_meses,
total_vlr_pedidos_entregues_ultimos_3_meses/qtd_pedidos_entregues_ultimos_3_meses as media_valores_pedidos_entregues_ultimos_3_meses,
total_vlr_pedidos_entregues_ultimos_4_meses/qtd_pedidos_entregues_ultimos_4_meses as media_valores_pedidos_entregues_ultimos_4_meses,
total_vlr_pedidos_entregues_ultimos_5_meses/qtd_pedidos_entregues_ultimos_5_meses as media_valores_pedidos_entregues_ultimos_5_meses,
total_vlr_pedidos_entregues_ultimos_6_meses/qtd_pedidos_entregues_ultimos_6_meses as media_valores_pedidos_entregues_ultimos_6_meses,

total_vlr_pedidos_cancelados_ultimo_mes/qtd_pedidos_cancelados_ultimo_mes as media_valores_pedidos_cancelados_ultimo_mes,
total_vlr_pedidos_cancelados_ultimos_2_meses/qtd_pedidos_cancelados_ultimos_2_meses as media_valores_pedidos_cancelados_ultimos_2_meses,
total_vlr_pedidos_cancelados_ultimos_3_meses/qtd_pedidos_cancelados_ultimos_3_meses as media_valores_pedidos_cancelados_ultimos_3_meses,
total_vlr_pedidos_cancelados_ultimos_4_meses/qtd_pedidos_cancelados_ultimos_4_meses as media_valores_pedidos_cancelados_ultimos_4_meses,
total_vlr_pedidos_cancelados_ultimos_5_meses/qtd_pedidos_cancelados_ultimos_5_meses as media_valores_pedidos_cancelados_ultimos_5_meses,
total_vlr_pedidos_cancelados_ultimos_6_meses/qtd_pedidos_cancelados_ultimos_6_meses as media_valores_pedidos_cancelados_ultimos_6_meses,

qtd_pedidos_dt_prevista_menor_data_entrega_ate_hoje,
qtd_pedidos_dt_prevista_menor_data_entrega_ultimo_mes,
qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_2_meses,
qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_3_meses,
qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_4_meses,
qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_5_meses,
qtd_pedidos_dt_prevista_menor_data_entrega_ultimos_6_meses,

qtd_pedidos_pgto_cc_ultimo_mes,
qtd_pedidos_pgto_cc_ultimos_2_meses,
qtd_pedidos_pgto_cc_ultimos_3_meses,
qtd_pedidos_pgto_cc_ultimos_4_meses,
qtd_pedidos_pgto_cc_ultimos_5_meses,
qtd_pedidos_pgto_cc_ultimos_6_meses,

qtd_pedidos_pgto_boleto_ultimo_mes,
qtd_pedidos_pgto_boleto_ultimos_2_meses,
qtd_pedidos_pgto_boleto_ultimos_3_meses,
qtd_pedidos_pgto_boleto_ultimos_4_meses,
qtd_pedidos_pgto_boleto_ultimos_5_meses,
qtd_pedidos_pgto_boleto_ultimos_6_meses,

qtd_pedidos_pgto_outros_ultimo_mes,
qtd_pedidos_pgto_outros_ultimos_2_meses,
qtd_pedidos_pgto_outros_ultimos_3_meses,
qtd_pedidos_pgto_outros_ultimos_4_meses,
qtd_pedidos_pgto_outros_ultimos_5_meses,
qtd_pedidos_pgto_outros_ultimos_6_meses,

media_qtd_parcelas_pedidos_pgto_cc_ultimo_mes,
media_qtd_parcelas_pedidos_pgto_cc_ultimos_2_meses,
media_qtd_parcelas_pedidos_pgto_cc_ultimos_3_meses,
media_qtd_parcelas_pedidos_pgto_cc_ultimos_4_meses,
media_qtd_parcelas_pedidos_pgto_cc_ultimos_5_meses,
media_qtd_parcelas_pedidos_pgto_cc_ultimos_6_meses,

media_qtd_parcelas_pedidos_pgto_voucher_ultimo_mes,
media_qtd_parcelas_pedidos_pgto_voucher_ultimos_2_meses,
media_qtd_parcelas_pedidos_pgto_voucher_ultimos_3_meses,
media_qtd_parcelas_pedidos_pgto_voucher_ultimos_4_meses,
media_qtd_parcelas_pedidos_pgto_voucher_ultimos_5_meses,
media_qtd_parcelas_pedidos_pgto_voucher_ultimos_6_meses,

max_qtd_parcelas_pedidos_pgto_cc_ultimo_mes,
max_qtd_parcelas_pedidos_pgto_cc_ultimos_2_meses,
max_qtd_parcelas_pedidos_pgto_cc_ultimos_3_meses,
max_qtd_parcelas_pedidos_pgto_cc_ultimos_4_meses,
max_qtd_parcelas_pedidos_pgto_cc_ultimos_5_meses,
max_qtd_parcelas_pedidos_pgto_cc_ultimos_6_meses,

max_qtd_parcelas_pedidos_pgto_voucher_ultimo_mes,
max_qtd_parcelas_pedidos_pgto_voucher_ultimos_2_meses,
max_qtd_parcelas_pedidos_pgto_voucher_ultimos_3_meses,
max_qtd_parcelas_pedidos_pgto_voucher_ultimos_4_meses,
max_qtd_parcelas_pedidos_pgto_voucher_ultimos_5_meses,
max_qtd_parcelas_pedidos_pgto_voucher_ultimos_6_meses,

total_valor_pedidos_pgto_cc_ultimo_mes/qtd_pedidos_pgto_cc_ultimo_mes as media_valor_pedidos_pgto_cc_ultimo_mes,
total_valor_pedidos_pgto_cc_ultimos_2_meses/qtd_pedidos_pgto_cc_ultimos_2_meses as media_valor_pedidos_pgto_cc_ultimos_2_meses,
total_valor_pedidos_pgto_cc_ultimos_3_meses/qtd_pedidos_pgto_cc_ultimos_3_meses as media_valor_pedidos_pgto_cc_ultimos_3_meses,
total_valor_pedidos_pgto_cc_ultimos_4_meses/qtd_pedidos_pgto_cc_ultimos_4_meses as media_valor_pedidos_pgto_cc_ultimos_4_meses,
total_valor_pedidos_pgto_cc_ultimos_5_meses/qtd_pedidos_pgto_cc_ultimos_5_meses as media_valor_pedidos_pgto_cc_ultimos_5_meses,
total_valor_pedidos_pgto_cc_ultimos_6_meses/qtd_pedidos_pgto_cc_ultimos_6_meses as media_valor_pedidos_pgto_cc_ultimos_6_meses,

total_valor_pedidos_pgto_boleto_ultimo_mes/qtd_pedidos_pgto_boleto_ultimo_mes as media_valor_pedidos_pgto_boleto_ultimo_mes,
total_valor_pedidos_pgto_boleto_ultimos_2_meses/qtd_pedidos_pgto_boleto_ultimos_2_meses as media_valor_pedidos_pgto_boleto_ultimos_2_meses,
total_valor_pedidos_pgto_boleto_ultimos_3_meses/qtd_pedidos_pgto_boleto_ultimos_3_meses as media_valor_pedidos_pgto_boleto_ultimos_3_meses,
total_valor_pedidos_pgto_boleto_ultimos_4_meses/qtd_pedidos_pgto_boleto_ultimos_4_meses as media_valor_pedidos_pgto_boleto_ultimos_4_meses,
total_valor_pedidos_pgto_boleto_ultimos_5_meses/qtd_pedidos_pgto_boleto_ultimos_5_meses as media_valor_pedidos_pgto_boleto_ultimos_5_meses,
total_valor_pedidos_pgto_boleto_ultimos_6_meses/qtd_pedidos_pgto_boleto_ultimos_6_meses as media_valor_pedidos_pgto_boleto_ultimos_6_meses,

total_valor_pedidos_pgto_outros_ultimo_mes/qtd_pedidos_pgto_outros_ultimo_mes as media_valor_pedidos_pgto_outros_ultimo_mes,
total_valor_pedidos_pgto_outros_ultimos_2_meses/qtd_pedidos_pgto_outros_ultimos_2_meses as media_valor_pedidos_pgto_outros_ultimos_2_meses,
total_valor_pedidos_pgto_outros_ultimos_3_meses/qtd_pedidos_pgto_outros_ultimos_3_meses as media_valor_pedidos_pgto_outros_ultimos_3_meses,
total_valor_pedidos_pgto_outros_ultimos_4_meses/qtd_pedidos_pgto_outros_ultimos_4_meses as media_valor_pedidos_pgto_outros_ultimos_4_meses,
total_valor_pedidos_pgto_outros_ultimos_5_meses/qtd_pedidos_pgto_outros_ultimos_5_meses as media_valor_pedidos_pgto_outros_ultimos_5_meses,
total_valor_pedidos_pgto_outros_ultimos_6_meses/qtd_pedidos_pgto_outros_ultimos_6_meses as media_valor_pedidos_pgto_outros_ultimos_6_meses,

prd.quantidade_de_produtos_por_categoria,
prd.media_num_fotos_por_produto,
prd.media_fotos_por_produto_categoria,
prd.media_tamanho_descriçao_por_produto,
prd.qtd_categorias_vendendo,

qtd_review_ate_hoje,
media_notas_review_ate_hoje,
media_dias_entre_criacao_resposta_review_ate_hoje,

qtd_review_ultimo_mes,
qtd_review_ultimos_2_meses,
qtd_review_ultimos_3_meses,
qtd_review_ultimos_4_meses,
qtd_review_ultimos_5_meses,
qtd_review_ultimos_6_meses,

(1.0*qtd_review_ultimo_mes) / qtd_review_ultimos_3_meses as tend_qtd_notas_review_m1_m3,
(1.0*qtd_review_ultimo_mes) / qtd_review_ultimos_6_meses as tend_qtd_notas_review_m1_m6,
(1.0*qtd_review_ultimos_3_meses) / qtd_review_ultimos_6_meses as tend_qtd_notas_review_m3_m6,

media_notas_review_ultimo_mes,
media_notas_review_ultimos_2_meses,
media_notas_review_ultimos_3_meses,
media_notas_review_ultimos_4_meses,
media_notas_review_ultimos_5_meses,
media_notas_review_ultimos_6_meses,

(1.0*media_notas_review_ultimo_mes) / media_notas_review_ultimos_3_meses as tend_notas_review_m1_m3,
(1.0*media_notas_review_ultimo_mes) / media_notas_review_ultimos_6_meses as tend_notas_review_m1_m6,
(1.0*media_notas_review_ultimos_3_meses) / media_notas_review_ultimos_6_meses as tend_notas_review_m3_m6,

media_dias_entre_criacao_resposta_review_ultimo_mes,
media_dias_entre_criacao_resposta_review_ultimos_2_meses,
media_dias_entre_criacao_resposta_review_ultimos_3_meses,
media_dias_entre_criacao_resposta_review_ultimos_4_meses,
media_dias_entre_criacao_resposta_review_ultimos_5_meses,
media_dias_entre_criacao_resposta_review_ultimos_6_meses

from vw_olist_abt_p1 as vwabt
inner join olist_sellers_dataset sel -- para buscar CEP e cidade
on (vwabt.seller_id = sel.seller_id) 
inner join olist_products_preabt prd -- join com preabts com agrupamento por seller
on (vwabt.seller_id = prd.seller_id)
left join olist_flvendas_preabt flv
on (vwabt.seller_id = flv.seller_id);

-- Fim seção das views


