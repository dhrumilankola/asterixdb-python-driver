{% if if_not_exists %}
CREATE TYPE IF NOT EXISTS {{ name }} AS {% if not open_type %}CLOSED {% endif %}{
    {{ definition }}
};
{% else %}
CREATE TYPE {{ name }} AS {% if not open_type %}CLOSED {% endif %}{
    {{ definition }}
};
{% endif %}