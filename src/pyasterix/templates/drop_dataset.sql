{% if if_exists %}
DROP DATASET {{ current_dataverse }}.{{ name }} IF EXISTS;
{% else %}
DROP DATASET {{ current_dataverse }}.{{ name }};
{% endif %}