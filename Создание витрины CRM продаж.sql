-- Создание витрины CRM продаж
-- Выделяются следующие признаки: выручка по сделке, статус оплаты, состояние воронки продаж, количество счетов, идентификаторы ответственных сотрудников и компаний

CREATE TABLE mart_crm_sales AS

WITH deal_base AS (
    SELECT
        deal.id AS deal_id,
        deal.company_id,
        deal.assigned_by_id,
        deal.stage_semantic_id,
        deal.opportunity,
        deal.date_create::date AS deal_create_date,
        deal.closedate::date AS deal_plan_close_date
    FROM v3_crm_deal deal
    WHERE NOT deal.deleted
),

invoice_base AS (
    SELECT
        id AS invoice_id,
        uf_deal_id AS deal_id,
        price AS invoice_price,
        pay_voucher_date::date AS payment_date,
        status_id,
        payed
    FROM v3_crm_invoice
    WHERE NOT deleted
),

revenue_logic AS (
    SELECT
        d.deal_id,
        d.company_id,
        d.assigned_by_id,
        d.deal_create_date,
        d.deal_plan_close_date,
        i.invoice_id,
        i.invoice_price,

        -- Определение выручки (только оплаченные счета)
        CASE
            WHEN i.status_id = 'P' THEN COALESCE(i.invoice_price, 0)
            ELSE 0
        END AS revenue,

        -- Флаг успешной сделки
        CASE
            WHEN d.stage_semantic_id = 'S' THEN 1
            ELSE 0
        END AS is_success_deal,

        -- Флаг оплаты
        CASE
            WHEN i.status_id = 'P' THEN 1
            ELSE 0
        END AS is_paid

    FROM deal_base d
    LEFT JOIN invoice_base i
        ON d.deal_id = i.deal_id
),

final_mart AS (
    SELECT
        deal_id, -- ID сделки
        company_id, -- ID компании
        assigned_by_id, -- ID ответственного
        deal_create_date, -- дата создания сделки
        deal_plan_close_date, -- планируемая дата закрытия сделки
        invoice_id, -- ID счета

        SUM(revenue) AS revenue, -- Суммарная выручка по сделке
        MAX(is_success_deal) AS deal_success_flag, -- Флаг успешной сделки
        MAX(is_paid) AS payment_flag, -- Флаг оплаты

        COUNT(DISTINCT invoice_id) AS invoice_cnt,

        -- Состояние воронки продаж
        CASE
            WHEN MAX(is_success_deal) = 1 THEN 'SUCCESS'
            WHEN MAX(is_paid) = 1 THEN 'PAID_NOT_SUCCESS'
            ELSE 'IN_PROGRESS'
        END AS deal_funnel_state

    FROM revenue_logic
    GROUP BY
        deal_id,
        company_id,
        assigned_by_id,
        deal_create_date,
        deal_plan_close_date,
        invoice_id
)

SELECT *
FROM final_mart;