{% test integer(model, column_name) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and try_cast({{ column_name }} as bigint) is null

{% endtest %}
