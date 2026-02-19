{% test financial_total_check(model, column_name) %}

    select
        {{ column_name }},
        financial_base,
        financial_extras
    from {{ model }}
    where abs({{ column_name }} - (financial_base + financial_extras)) > 0.01

{% endtest %}
