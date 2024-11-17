{% if if_exists %}
DROP TYPE {{ current_dataverse }}.{{ name }} IF EXISTS;
{% else %}
DROP TYPE {{ current_dataverse }}.{{ name }};
{% endif %}