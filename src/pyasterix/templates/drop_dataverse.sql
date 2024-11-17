{% if if_exists %}
DROP DATAVERSE IF EXISTS {{ name }};
{% else %}
DROP DATAVERSE {{ name }};
{% endif %}