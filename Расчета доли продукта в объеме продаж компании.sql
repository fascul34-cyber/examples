--В коде предстален небольшой пример ETL пайплайна для расчета доли продукта в объеме продаж компании.
--Так как мы показываем вклад отдельного продукта в общий продуктовый портфель, то подобное решение может использоваться для распределения затрат/бюджетов, анализа динамики продуктовой линейки, расчета инвестиционных приоритетов и так далее. 
--Код поделен на блоки для удобства чтения.

-- 1. Контекст расчёта (параметры версии). Вычисляем дату, год, месяц и флаг конца года.
--Далее мы будем использовать как доп. атрибут для витрин.
CREATE TEMP TABLE tmp_version_context AS
SELECT
      current_date AS calc_date,
      EXTRACT(YEAR FROM current_date) AS year,
      EXTRACT(MONTH FROM current_date) AS month,
      CASE WHEN EXTRACT(MONTH FROM current_date) = 12 THEN 1 ELSE 0 END AS is_year_end;

-- 2. Исторические данные продаж (исходный слой).
CREATE TEMP TABLE tmp_sales_raw AS
SELECT
      s.company_id,
      s.product_id,
      s.sale_month,
      s.amount
FROM source_sales s
WHERE s.sale_month >= DATE_TRUNC('month', current_date) - INTERVAL '12 month';

-- Создаем индекс для ускорения соединенй.
CREATE INDEX idx_sales_raw_company ON tmp_sales_raw(company_id);


-- 3. Вычисление сумм по компаниям и продуктам, а также группировка.
CREATE TEMP TABLE tmp_sales_agg AS
SELECT
      company_id,
      product_id,
      SUM(amount) AS total_amount
FROM tmp_sales_raw
GROUP BY 1,2;

-- 4. Общая сумма по компании (нужна для нормировки)
CREATE TEMP TABLE tmp_company_totals AS
SELECT
      company_id,
      SUM(total_amount) AS company_total
FROM tmp_sales_agg
GROUP BY 1;

-- 5. Расчет доли продукта
CREATE TEMP TABLE tmp_distribution_drivers AS
SELECT
      a.company_id,
      a.product_id,
      a.total_amount,
      t.company_total,
      a.total_amount / NULLIF(t.company_total, 0) AS driver_ratio
FROM tmp_sales_agg a
JOIN tmp_company_totals t USING (company_id);


-- 6. Финальная витрина
CREATE TEMP TABLE final_distribution AS
SELECT
      d.company_id,
      d.product_id,
      d.driver_ratio,
      v.year,
      v.month,
      v.is_year_end
FROM tmp_distribution_drivers d
CROSS JOIN tmp_version_context v;


-- В итоге, получается таблица final_distribution, которая содержит распределительные коэффициенты

SELECT * FROM final_distribution;
