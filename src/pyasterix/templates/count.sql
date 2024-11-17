SELECT COUNT(*) as count
FROM {{ dataset }}
{% if where_clause %}
WHERE {{ where_clause }}
{% endif %}