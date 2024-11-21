USE {{current_dataverse}};
DROP DATASET {% if if_exists %}IF EXISTS {% endif %}{{name}};