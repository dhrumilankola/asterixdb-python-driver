{% if if_not_exists %}
CREATE DATAVERSE IF NOT EXISTS {{ name }};
{% else %}
CREATE DATAVERSE {{ name }};
{% endif %}