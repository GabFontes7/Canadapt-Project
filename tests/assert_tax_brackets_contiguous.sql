with ordered as (
    select
        jurisdiction,
        province_code,
        bracket_order,
        lower_threshold,
        upper_threshold,
        lead(lower_threshold) over (
            partition by jurisdiction, province_code
            order by bracket_order
        ) as next_lower,
        row_number() over (
            partition by jurisdiction, province_code
            order by bracket_order desc
        ) as reverse_order
    from {{ ref('tax_brackets_2026') }}
)

select *
from ordered
where lower_threshold < 0
   or (reverse_order > 1 and upper_threshold is null)
   or (reverse_order = 1 and upper_threshold is not null)
   or (next_lower is not null and upper_threshold <> next_lower)
