{% if if_not_exists %}
CREATE DATASET IF NOT EXISTS {{ current_dataverse }}.{{ name }}({{ type_name }})
    PRIMARY KEY {{ primary_key }};
{% else %}
CREATE INTERNAL DATASET {{ current_dataverse }}.{{ name }}({{ type_name }})
    PRIMARY KEY {{ primary_key }};
{% endif %}