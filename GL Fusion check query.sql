GL_Fusion Check :

(select  HFM_ENTITY,gl_period,gl_scenario,hfm_account,hfm_category,gl_account,gl_cost_centre_code,gl_cost_centre_name,finance_year_period_number,
round(gl_closing_balance,2)
from(
 select ENTITY as HFM_ENTITY,
       period as gl_period,
       scenario as gl_scenario,
       target_account as hfm_account,
       target_ledger as hfm_category,
       source_account as gl_account,
       source_cost_centre as gl_cost_centre_code,
       source_cost_centre_name as gl_cost_centre_name,
       sum(ob_debit) as gl_ob_debit,
       sum(ob_credit) as gl_ob_credit,
       sum(movement_debit) as gl_movement_debit,
       sum(movement_credit)as gl_movement_credit,
       sum(periodic_movement) as gl_periodic_movement ,
       sum(closing_balance) as gl_closing_balance,
       source_extracted_date as gl_last_extracted,
       concat(substr(scenario,1,4),period) as finance_year_period_number,
       year_period_number
from (
    select CASE 
               WHEN a.target_entity = 'SB1109' THEN 'SB1109' 
               WHEN a.target_entity = 'SB1151' THEN 'SB1151' 
               ELSE 'UKCORE_CF'  
           END AS ENTITY,
           a.target_account,
           a.target_ledger,
           a.source_account,
           case 
               when d.root= 'TBAL0' then null
               else a.source_cost_centre 
           end as source_cost_centre,
           case 
               when d.root= 'TBAL0' then null 
               when  a.source_cost_centre = 0 then null
               else e.description 
           end as source_cost_centre_name,           
           b.period, 
           b.scenario,
           a.year_period_number,
           b.source_extracted_date,
           c.ob_debit,
           c.ob_credit,
           c.movement_debit,
           c.movement_credit,
           c.periodic_movement,
           c.closing_balance 
    from delta_silver.dim_fdmee_mapping_slv a 
    inner join (
        select distinct fusion_coa_account,
                        substr(balance_period_NAME,1,2) as period,
                        concat(balance_period_year,"ACT") as scenario,
                        year_period_number,
                        max(source_extracted_date) over(partition by year_period_number ) as source_extracted_date
        from delta_silver.fact_fusion_glbalances_slv 
        where year_period_number =202310 
          and ledger_id = '300000598726042'
    ) b on b.fusion_coa_account=a.source_account 
    inner join delta_silver.fact_fdmee_transformed_data c on c.fdmee_fusion_string =a.fusion_string 
    inner join (
        select distinct fusion_code, root 
        from delta_silver.dim_fusion_account_hierarchy_slv 
        where year_period_number =202310 
          and base = 'True' 
          and root  in ('TPAL0', 'TBAL0')
    ) d on a.source_account =d.fusion_code 
    inner join (
        select distinct fusion_code ,description 
        from delta_silver.dim_fusion_costcentre_slv 
        where year_period_number =202310 
    ) e on (case when a.source_cost_centre = 0 then  '0000000' else a.source_cost_centre end ) =e.fusion_code
    where a.year_period_number=202310
    and a.source_ledger ='GB TESCO PRIMARY'
      and a.target_entity in (
          select DISTINCT NODE 
          from delta_silver.dim_hfm_entity_hierarchy_slv 
          where year_period_number =202310 
            and parent ='UKCORE_TOT' 
            and NODE NOT IN ('SB4001', 'SB4002', 'SB4003', 'SB4006')
      ) 
      and a.source_movement NOT like 'L%' 
      and a.source_account in (
          select distinct fusion_code 
          from delta_silver.dim_fusion_account_hierarchy_slv 
          where year_period_number =202310 
            and base = 'True' 
            and root  in ('TPAL0', 'TBAL0') 
      )
      and c.year_period_number =202310 
) 
group by ENTITY,
         target_account,
         target_ledger,
         source_account,
         source_cost_centre,
         source_cost_centre_name,
         period,
         scenario,
         source_extracted_date,
         concat(substr(scenario,1,4),period) ,
         year_period_number)

minus
(select hfm_entity,gl_period ,gl_scenario,hfm_account,hfm_category,gl_account,gl_cost_centre_code,gl_cost_centre_name,finance_year_period_number,
round(gl_closing_balance,2)
 from 
from delta_gold.fusion_gl_balances)

union

(select hfm_entity,gl_period ,gl_scenario,hfm_account,hfm_category,gl_account,gl_cost_centre_code,gl_cost_centre_name,finance_year_period_number,
round(gl_closing_balance,2)
 from 
from delta_gold.fusion_gl_balances)
)
minus

(select  HFM_ENTITY,gl_period,gl_scenario,hfm_account,hfm_category,gl_account,gl_cost_centre_code,gl_cost_centre_name,finance_year_period_number,
round(gl_closing_balance,2)
from(
 select ENTITY as HFM_ENTITY,
       period as gl_period,
       scenario as gl_scenario,
       target_account as hfm_account,
       target_ledger as hfm_category,
       source_account as gl_account,
       source_cost_centre as gl_cost_centre_code,
       source_cost_centre_name as gl_cost_centre_name,
       sum(ob_debit) as gl_ob_debit,
       sum(ob_credit) as gl_ob_credit,
       sum(movement_debit) as gl_movement_debit,
       sum(movement_credit)as gl_movement_credit,
       sum(periodic_movement) as gl_periodic_movement ,
       sum(closing_balance) as gl_closing_balance,
       source_extracted_date as gl_last_extracted,
       concat(substr(scenario,1,4),period) as finance_year_period_number,
       year_period_number
from (
    select CASE 
               WHEN a.target_entity = 'SB1109' THEN 'SB1109' 
               WHEN a.target_entity = 'SB1151' THEN 'SB1151' 
               ELSE 'UKCORE_CF'  
           END AS ENTITY,
           a.target_account,
           a.target_ledger,
           a.source_account,
           case 
               when d.root= 'TBAL0' then null
               else a.source_cost_centre 
           end as source_cost_centre,
           case 
               when d.root= 'TBAL0' then null 
               when  a.source_cost_centre = 0 then null
               else e.description 
           end as source_cost_centre_name,           
           b.period, 
           b.scenario,
           a.year_period_number,
           b.source_extracted_date,
           c.ob_debit,
           c.ob_credit,
           c.movement_debit,
           c.movement_credit,
           c.periodic_movement,
           c.closing_balance 
    from delta_silver.dim_fdmee_mapping_slv a 
    inner join (
        select distinct fusion_coa_account,
                        substr(balance_period_NAME,1,2) as period,
                        concat(balance_period_year,"ACT") as scenario,
                        year_period_number,
                        max(source_extracted_date) over(partition by year_period_number ) as source_extracted_date
        from delta_silver.fact_fusion_glbalances_slv 
        where year_period_number =202310 
          and ledger_id = '300000598726042'
    ) b on b.fusion_coa_account=a.source_account 
    inner join delta_silver.fact_fdmee_transformed_data c on c.fdmee_fusion_string =a.fusion_string 
    inner join (
        select distinct fusion_code, root 
        from delta_silver.dim_fusion_account_hierarchy_slv 
        where year_period_number =202310 
          and base = 'True' 
          and root  in ('TPAL0', 'TBAL0')
    ) d on a.source_account =d.fusion_code 
    inner join (
        select distinct fusion_code ,description 
        from delta_silver.dim_fusion_costcentre_slv 
        where year_period_number =202310 
    ) e on (case when a.source_cost_centre = 0 then  '0000000' else a.source_cost_centre end ) =e.fusion_code
    where a.year_period_number=202310
    and a.source_ledger ='GB TESCO PRIMARY'
      and a.target_entity in (
          select DISTINCT NODE 
          from delta_silver.dim_hfm_entity_hierarchy_slv 
          where year_period_number =202310 
            and parent ='UKCORE_TOT' 
            and NODE NOT IN ('SB4001', 'SB4002', 'SB4003', 'SB4006')
      ) 
      and a.source_movement NOT like 'L%' 
      and a.source_account in (
          select distinct fusion_code 
          from delta_silver.dim_fusion_account_hierarchy_slv 
          where year_period_number =202310 
            and base = 'True' 
            and root  in ('TPAL0', 'TBAL0') 
      )
      and c.year_period_number =202310 
) 
group by ENTITY,
         target_account,
         target_ledger,
         source_account,
         source_cost_centre,
         source_cost_centre_name,
         period,
         scenario,
         source_extracted_date,
         concat(substr(scenario,1,4),period) ,
         year_period_number)
)

 
