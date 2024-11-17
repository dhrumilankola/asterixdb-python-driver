-- insert.sql template
USE {{ current_dataverse }};
INSERT INTO {{ dataset }} 
(
    [
    {% for record in records %}{{ record | safe }}{% if not loop.last %},{% endif %}
    {% endfor %}
    ]
);
