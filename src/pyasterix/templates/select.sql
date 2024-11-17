SELECT {{ select_clause }}
FROM {{ dataset }}
{% if where_clause %}
WHERE {{ where_clause }}
{% endif %}
{% if order_clause %}
ORDER BY {{ order_clause }}
{% endif %}
{% if limit is not none %}
LIMIT {{ limit }}
{% endif %}
{% if offset is not none %}
OFFSET {{ offset }}
{% endif %}