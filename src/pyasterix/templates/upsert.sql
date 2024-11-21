USE {{current_dataverse}};
UPSERT INTO {{dataset}} 
(SELECT VALUE {
    "id": orig.id,
    "name": {% if "name" in updates %}{{updates.name}}{% else %}orig.name{% endif %},
    "email": {% if "email" in updates %}{{updates.email}}{% else %}orig.email{% endif %}
}
FROM {{dataset}} orig
WHERE {{where_clause}});