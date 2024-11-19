USE {{current_dataverse}};
DROP TYPE {% if if_exists %}IF EXISTS {% endif %}{{name}};